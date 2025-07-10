#include <stdio.h>
#include <string.h>

int normalize_path(const char *input, char *output, int out_size) {
    if (!input || !output || out_size <= 0) return -1;

    int len = strlen(input);
    if (len >= out_size) return -2;

    int out_idx = 0;
    int i = 0;
    while (i < len) {
        // Skip the unnecessary '/'
        while (input[i] == '/' && input[i+1] == '/') i++;

        // Skip "/./"
        if (input[i] == '/' && input[i+1] == '.' && input[i+2] == '/' ) {
            i += 2;
            continue;
        }

        // Copy characters to output
        if (out_idx < out_size - 1) {
            output[out_idx++] = input[i++];
        } else {
            return -3;  // Prevent overflow
        }
    }

    // Add end marker
    output[out_idx] = '\0';
    return 0;
}

// Test case:
// Case 1:
void test_normal_case() {
    char input[] = "/home//user/./docs/";
    char output[256];
    int status = normalize_path(input, output, sizeof(output));
    printf("Input: %s\nOutput: %s\n", input, output);
    if (status == 0 && strcmp(output, "/home/user/docs/") == 0) {
        printf("test_normal_case passed\n");
    } else {
        printf("test_normal_case failed\n");
    }
}

// Case 2:
void test_empty_input() {
    char output[256];
    int status = normalize_path(NULL, output, sizeof(output));
    if (status == -1) {
        printf("test_empty_input passed\n");
    } else {
        printf("test_empty_input failed\n");
    }
}

// Case 3:
void test_overflow_case() {
    char input[1024];
    memset(input, '/', sizeof(input)-1);
    input[sizeof(input)-1] = '\0';
    char output[16];
    int status = normalize_path(input, output, sizeof(output));
    if (status == -2 || status == -3) {
        printf("test_overflow_case passed\n");
    } else {
        printf("test_overflow_case failed\n");
    }
}

int main() {
    test_normal_case();
    test_empty_input();
    test_overflow_case();
    printf("All path normalization tests are complete.\n");
    return 0;
}
