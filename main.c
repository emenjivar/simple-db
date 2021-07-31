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


// Prototipe of functions
InputBuffer *new_input_buffer();
void read_input(InputBuffer *);
void print_prompt();
void destroy_input_buffer(InputBuffer *);

int main(int argc, char* argv[]) {
    InputBuffer *input = new_input_buffer();
    while(true) {
        print_prompt();
        read_input(input);

        if(strcmp(input->buffer, "exit") == 0) {
            printf("bye\n");
            destroy_input_buffer(input);
            exit(EXIT_SUCCESS);
        } else {
            printf("%s: command not found\n", input->buffer);
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