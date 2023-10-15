#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

int main() {
    int fd;
    char buffer[100];

   
    fd = open("test.txt", O_WRONLY | O_CREAT, 0666);
    if (fd < 0) {
        perror("Error opening file");
        return 1;
    }

    // Lock file 
    if (flock(fd, LOCK_EX|LOCK_NB) == -1) {
        perror("Error locking file");
        close(fd);
        return 1;
    }
   

   
    int pid = fork();
    if (pid == 0) {
      
        int i = 0;
        while (1) {
            // Write to file
            sprintf(buffer, "Child process: %d\n", i++);
            write(fd, buffer, strlen(buffer));
        }
    } else if (pid > 0) {
      
        int i = 0;
        while (1) {
            // Write to file
            sprintf(buffer, "Parent process: %d\n", i++);
            write(fd, buffer, strlen(buffer));
        }
    } else {
        // Error
        perror("Error spawning child process");
        close(fd);
        return 1;
    }

    // Release lock and close file
    flock(fd, LOCK_UN);
    close(fd);

    return 0;
}

