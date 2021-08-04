#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

// Structure to read command from prompt
typedef struct {
    char* buffer; // command string
    size_t buffer_length; // size of the allocated buffer
    ssize_t input_length; // number of bytes read
} InputBuffer;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

// meta commands: non-sql commands like .exit
typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

// state of statement
typedef enum { 
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TO_LONG,
    PREPARE_NEGATIVE_ID,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

// statement is the SQL compiler instructions
typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
    uint32_t id;  
    char username[COLUMN_USERNAME_SIZE + 1]; //add extra space to \0
    char email[COLUMN_EMAIL_SIZE + 1]; // add extra space to \0
} Row;

// Statement wrapper
typedef struct {
    StatementType type;
    Row row; // Only used by insert statement
} Statement;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

// Table constants definition
const uint32_t PAGE_SIZE = 4096; // 4 kilobytes
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
    uint32_t num_rows;
    void *pages[TABLE_MAX_PAGES];
} Table;

// Prototipe of functions
InputBuffer *new_input_buffer();
void read_input(InputBuffer *);
void print_prompt();
void destroy_input_buffer(InputBuffer *);
MetaCommandResult do_meta_command(InputBuffer *, Table *);
PrepareResult prepare_insert(InputBuffer *, Statement *);
PrepareResult prepare_statement(InputBuffer *, Statement *);
ExecuteResult execute_insert(Statement *, Table *);
ExecuteResult execute_select(Statement *, Table *);
ExecuteResult execute_statement(Statement *, Table *);
void serialize_row(Row *, void* );
void deserialize_row(void *, Row *);
void *row_slot(Table *table, uint32_t row_num);
void print_row(Row *);
Table *init_table();
void free_table(Table *);

int main(int argc, char* argv[]) {
    Table *table = init_table();
    InputBuffer *input = new_input_buffer();
    while(true) {
        print_prompt();
        read_input(input);

        // Meta commands
        if(input->buffer[0] == '.') {
            switch (do_meta_command(input, table)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    printf("%s: command not found\n", input->buffer);
                    continue;
            }
        } 
        // Produce an empty line on the prompt
        else if(strcmp(input->buffer, "") == 0) {
            continue;
        }

        Statement statement;
        // SQL statement
        switch(prepare_statement(input, &statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_SYNTAX_ERROR:
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case PREPARE_STRING_TO_LONG:
                printf("String to long.\n");
                continue;
            case PREPARE_NEGATIVE_ID:
                printf("ID must be positive.\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("%s: unrecognized keyword\n", input->buffer);
                continue; // Move from the begining of loop
       }

       switch(execute_statement(&statement, table)) {
            case EXECUTE_SUCCESS:
                printf("Executed.\n");
                break;
            case EXECUTE_TABLE_FULL:
                printf("Error: Table full.\n");
                break;
       }
    }
}

// Initialize the buffer
InputBuffer *new_input_buffer() {
    InputBuffer *input = (InputBuffer*) malloc(sizeof(InputBuffer));
    input->buffer = NULL;
    input->buffer_length = 0;
    input->input_length = 0;
    return input;
}

void read_input(InputBuffer *input) {
    ssize_t bytes_read = getline(&(input->buffer), &(input->buffer_length), stdin);

    if(bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // Ignore newline character
    input->input_length = bytes_read - 1;
    input->buffer[bytes_read - 1] = 0;
}

void print_prompt() {
    printf("db > ");
}

void destroy_input_buffer(InputBuffer *input) {
    free(input->buffer);
    free(input);
}

MetaCommandResult do_meta_command(InputBuffer *input,Table *table) {
    if(strcmp(input->buffer, ".exit") == 0) {
        printf("Bye.\n");
        destroy_input_buffer(input);
        free_table(table);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_insert(InputBuffer *input, Statement *statement) {
    statement->type = STATEMENT_INSERT;

    char *keyword = strtok(input->buffer, " ");
    char *sid = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    if(sid == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(sid);

    //validattions
    if(id <= 0) {
        return PREPARE_NEGATIVE_ID; 
    }

    if(strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TO_LONG;
    }

    if(strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TO_LONG;
    }
    //end of validations

    statement->row.id = id;
    strcpy(statement->row.username, username);
    strcpy(statement->row.email, email);

    return PREPARE_SUCCESS;
}
/**
 * Before execute SQL statement, parse input command 
 */
PrepareResult prepare_statement(InputBuffer *input, Statement *statement) {
    if(strncmp(input->buffer, "insert", 6) == 0) {
        return prepare_insert(input, statement);
    }
    if(strcmp(input->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement *statement, Table *table) {
    if(table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    Row *row = &(statement->row);
    serialize_row(row, row_slot(table, table->num_rows));
    table->num_rows += 1;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table) {
    Row row;

    if(table->num_rows == 0) {
        printf("Empty table.\n");
        return EXIT_SUCCESS;
    }

    for(uint32_t i=0; i<table->num_rows; i++) {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    printf("%d rows printed.\n", table->num_rows);
    return EXECUTE_SUCCESS;
}

/**
 * Select the type of statement to execute
 */
ExecuteResult execute_statement(Statement *statement, Table *table) {
    switch (statement->type) {
        case STATEMENT_INSERT:
            return execute_insert(statement, table);
        case STATEMENT_SELECT: 
            return execute_select(statement, table);
    }
}

/**
 * This function save a row on memory.
 * @param destination is an address memory from which we start storing fields.
 */
void serialize_row(Row *source, void* destination) {
    /**
     * row is stored in a memory block, starting from *destination memory address
     *  _______________________________________________________________________________  
     * |           id          |          username          |         email            |
     * |   from *destination   |    from username_offset    |     from email_offset    |
     * |    to &source->id     |    to &source->username    |     to &source->email    |
     * |  with length id_size  |  with length username_size |  with length email_size  |
     */
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

/**
 * Get row fields
 * @param source is an address memory from with we start to get the value of fields.
 */
void deserialize_row(void *source, Row *destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);

}

/**
 * This function returns an address memory, 
 * which is destination parameter to serialize_row
 * and source paremeter to deserialize_row
 **/
void *row_slot(Table *table, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pages[page_num];
    
    if(page == NULL) {
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

void print_row(Row *row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

Table *init_table() {
    Table *table = malloc(sizeof(Table));
    table->num_rows = 0;

    for(uint32_t i=0; i<TABLE_MAX_PAGES; i++) {
        table->pages[i] = NULL;
    }

    return table;
}

void free_table(Table *table) {
    for(int i=0; table->pages[i]; i++) {
        free(table->pages[i]);
    }

    free(table);
}