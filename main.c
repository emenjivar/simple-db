#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

// Structure to read command from prompt
typedef struct {
    char* buffer; // command string
    size_t buffer_length; // size of the allocated buffer
    ssize_t input_length; // number of bytes read
} InputBuffer;

// meta commands: non-sql commands like .exit
typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

// state of statement
typedef enum { 
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

// statement is the SQL compiler instructions
typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

// statement wrapper
typedef struct {
    StatementType type;
} Statement;

// Prototipe of functions
InputBuffer *new_input_buffer();
void read_input(InputBuffer *);
void print_prompt();
void destroy_input_buffer(InputBuffer *);
MetaCommandResult do_meta_command(InputBuffer *);
PrepareResult prepare_statement(InputBuffer *, Statement *);
void execute_statement(Statement *);

int main(int argc, char* argv[]) {
    InputBuffer *input = new_input_buffer();
    while(true) {
        print_prompt();
        read_input(input);

        // Meta commands
        if(input->buffer[0] == '.') {
            switch (do_meta_command(input)) {
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
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("%s: unrecognized keyword\n", input->buffer);
                continue; // Move from the begining of loop
       }

       execute_statement(&statement);
       printf("Executed.\n");
    
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

MetaCommandResult do_meta_command(InputBuffer *input) {
    if(strcmp(input->buffer, ".exit") == 0) {
        printf("bye\n");
        destroy_input_buffer(input);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(InputBuffer *input, Statement *statement) {
    if(strncmp(input->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if(strcmp(input->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement *statement) {
    switch (statement->type) {
        case STATEMENT_INSERT:
            printf("This is where we would do an insert.\n");
            break;
        case STATEMENT_SELECT: 
            printf("This is where we would do a select.\n");
            break;
    }
}