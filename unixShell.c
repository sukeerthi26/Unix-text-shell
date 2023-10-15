#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdbool.h>
#include <signal.h>
#include <readline/readline.h>
#include <dirent.h>
#include <glob.h>
#include <time.h>
#include <math.h>


#define MAX_COMMANDS 100
#define MAX_ARGS 100
#define MAX_LEN 1000

#define READ 0
#define WRITE 1
#define MAX_LEVEL 10

FILE* ptr;
FILE* nfp;
int lineno,lineptr;

int wildcards(char *pattern, char *expanded[]) {
  int count = 0;
  glob_t results;
  if (glob(pattern, GLOB_NOCHECK | GLOB_TILDE, NULL, &results) == 0) {
    int i;
    for (i = 0; i < results.gl_pathc; i++) {
      expanded[count++] = strdup(results.gl_pathv[i]);
    }
    globfree(&results);
  }
  return count;
}

void execute_command(int argc, char *argv[]) {
  int i, j, argc1 = 0;
  char *argv1[MAX_ARGS];
  for (i = 0; i < argc-1; i++) {
    if (strchr(argv[i], '*') || strchr(argv[i], '?')) {
      int n = wildcards(argv[i], &argv1[argc1]);
      argc1 += n;
    } 
    else {
      argv1[argc1++] = argv[i];
    }
  }
  argv1[argc1++] = NULL;
  execvp(argv1[0], argv1);
}

void display_process_hierarchy(pid_t pid, int level, int suggest)
{
    pid_t ppid;
    int child_count = 0;
    time_t start_time;
    DIR *dir;
    struct dirent *entry;
    char path[50];
    FILE *fp;
    char buf[100];
    char cmd[100];
    char name[100];
    int i;

    if (level > MAX_LEVEL)
        return;

    sprintf(path, "/proc/%d/status", pid);
    fp = fopen(path, "r");
    if (fp == NULL)
    {
        perror("fopen");
        return;
    }

    while (fgets(buf, sizeof(buf), fp) != NULL)
    {
        if (strncmp(buf, "PPid:", 5) == 0)
        {
            sscanf(buf, "%s %d", cmd, &ppid);
            break;
        }
    }

    sprintf(path, "/proc/%d/task/%d/children", pid, pid);
    fp = fopen(path, "r");
    if (fp == NULL)
    {
        perror("fopen");
        return;
    }

    while (fgets(buf, sizeof(buf), fp) != NULL)
        child_count++;

    sprintf(path, "/proc/%d/stat", pid);
    fp = fopen(path, "r");
    if (fp == NULL)
    {
        perror("fopen");
        return;
    }

    fgets(buf, sizeof(buf), fp);
    sscanf(buf, "%d (%[^)]) %*c %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %ld",
           &i, name, &start_time);

    fclose(fp);

    for (i = 0; i < level; i++)
        printf("    ");
    printf("%d %s\n", pid, name);

    if (suggest)
    {
        if (child_count > 20 || (time(NULL) - start_time) > 10)
        {
            for (i = 0; i < level; i++)
                printf("    ");
            printf("***\n");
        }
    }

    pid_t child_pid = fork();
    if (child_pid == 0)
    {
        display_process_hierarchy(ppid, level + 1, suggest);
        exit(0);
    }
    else if (child_pid > 0)
    {
        wait(NULL);
    }
    else
    {
        perror("fork");
    }
}

int pid1;
int pno = 1;
int grpid;
bool background = false;
void sigintHandler(int sig_num)
{
    printf("\n");
}

void sigstpHandler(int sig_num)
{
    // This function is called when "Ctrl-Z" is pressed
    background = true;
    killpg(grpid, SIGCONT);
}

// cd command
int cd(char *s)
{
    char currdir[200];
    if (s==NULL)
    {
        chdir(getenv("HOME"));
        return 0;
    }
    if (chdir(s) != 0)
    {
        return 1;
    }
    return 0;
}

// Structure to store parsed components of the user command
struct cmd
{
    char *args[MAX_ARGS]; // arguments
    int argc;             // number of arguments
    char *infile;         // input file (if any)
    char *outfile;        // output file (if any)
    bool bg;              // background execution flag
};

void removeWhiteSpace(char *buf)
{
    if (buf[strlen(buf) - 1] == ' ' || buf[strlen(buf) - 1] == '\n')
        buf[strlen(buf) - 1] = '\0';
    if (buf[0] == ' ' || buf[0] == '\n')
        memmove(buf, buf + 1, strlen(buf));
}

void tokenize(char **argv, int *argc, char *buf, const char *c)
{
    char *token;
    token = strtok(buf, c);
    int size = -1;
    while (token)
    {
        argv[++size] = malloc(sizeof(char) * (strlen(token) + 1));
        strcpy(argv[size], token);
        removeWhiteSpace(argv[size]);
        token = strtok(NULL, c);
    }
    argv[++size] = NULL;
    *argc = size;}

struct cmd parse_cmd(char *cmdline, bool background)
{
    // printf("Cmd line is : %s\n", cmdline);
    char *param1[100], *param2[100], *param3[100];
    int n1 = 0, n2 = 0;
    struct cmd cmd;
    cmd.argc = 0;
    cmd.infile = NULL;
    cmd.outfile = NULL;
    cmd.bg = background;

    if (strchr(cmdline, '<') && strchr(cmdline, '>'))
    {
        int i;
        tokenize(param1, &n1, cmdline, "<");
        removeWhiteSpace(param1[0]);
        tokenize(param2, &n1, param1[0], " \t");
        for (i = 0; i < n1; i++)
        {
            cmd.args[cmd.argc++] = strdup(param2[i]);
        }
        removeWhiteSpace(param1[1]);
        tokenize(param2, &n1, param1[1], ">");

        removeWhiteSpace(param2[0]);
        tokenize(param3, &n1, param2[0], " \t");

        removeWhiteSpace(param3[0]);
        cmd.infile = strdup(param3[0]);
        for (i = 1; i < n1; i++)
        {
            cmd.args[cmd.argc++] = strdup(param3[i]);
        }
        removeWhiteSpace(param2[1]);
        cmd.outfile = strdup(param2[1]);
    }
    else if (strchr(cmdline, '<'))
    {
        int i;
        tokenize(param1, &n1, cmdline, "<");
        removeWhiteSpace(param1[0]);
        tokenize(param2, &n1, param1[0], " \t");
        for (i = 0; i < n1; i++)
        {
            cmd.args[cmd.argc++] = strdup(param2[i]);
        }
        removeWhiteSpace(param1[1]);
        cmd.infile = strdup(param1[1]);
    }
    else if (strchr(cmdline, '>'))
    {
        int i;
        tokenize(param1, &n1, cmdline, ">");
        removeWhiteSpace(param1[0]);
        tokenize(param2, &n1, param1[0], " \t");
        for (i = 0; i < n1; i++)
        {
            cmd.args[cmd.argc++] = strdup(param2[i]);
        }
        removeWhiteSpace(param1[1]);
        cmd.outfile = strdup(param1[1]);
    }
    else
    {
        tokenize(param1, &n1, cmdline, " ");
        for (int i = 0; i < n1; i++)
        {
            cmd.args[cmd.argc++] = strdup(param1[i]);
        }
    }
    
    return cmd;
}

struct cmd *parse_input(char input[], int *n)
{
    int num = 0;
    char *param[100];
    struct cmd *s;
    s = (struct cmd *)malloc(MAX_COMMANDS * sizeof(struct cmd));
    removeWhiteSpace(input);
    tokenize(param, &num, input, "|");
    *n = num;
    for (int i = 0; i < num; i++)
    {
        bool background = false;
        int n = strlen(param[i]);
        if (param[i][n - 1] == '&')
        {
            param[i][n - 1] = '\0';
            background = true;
        }
        removeWhiteSpace(param[i]);
        s[i] = parse_cmd(param[i], background);
    }
    return s;
}

static void cleanup(int n)
{
    int i;
    for (i = 0; i < n; ++i)
        wait(NULL);
}

void delep(char* filename) {
  
  int process_ids[1024];
  int open_process_ids[1024];
  int lock_process_ids[1024];
  int open_count = 0;
  int lock_count = 0;
  int process_count = 0;

  char *filepath = filename;
  int pid = fork();
  if (pid == 0) {
   
    pid_t child_pid = getpid();
    process_ids[process_count++] = child_pid;

    // Open file in each process
    FILE *fp = fopen(filepath, "r");
    if (fp != NULL) {
      open_process_ids[open_count++] = child_pid;
      fclose(fp);
    }

    // Try to lock file in each process
    int fd = open(filepath, O_RDONLY);
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
      lock_process_ids[lock_count++] = child_pid;
      close(fd);
    }

    // Print list of process IDs with file open or lock
    printf("Process IDs with file open:\n");
    for (int i = 0; i < open_count; i++) {
      printf("%d\n", open_process_ids[i]);
    }
    printf("\nProcess IDs with lock on file:\n");
    for (int i = 0; i < lock_count; i++) {
      printf("%d\n", lock_process_ids[i]);
    }
    exit(0);
  } else if (pid > 0) {
   
    int status;
    waitpid(pid, &status, 0);
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
      // Ask  permission for user to kill processes
      char answer[4];
      printf("Do you want to kill the above processes? (yes/no) ");
      scanf("%s", answer);
      if (answer[0] == 'y' || answer[0] == 'Y') {
        // Kill processes
        for (int i = 0; i < open_count; i++) {
          kill(open_process_ids[i], SIGKILL);
        }
        for (int i = 0; i < lock_count; i++) {
          kill(lock_process_ids[i], SIGKILL);
        }
        // Delete file
        if (remove(filepath) == 0) {
          printf("File successfully deleted.\n");
        }
      }
    }
  } else if (pid < 0) {
    perror("fork");
    exit(0);
  }
  return ;
}

void ctrl_a_handler(int count, int key) {
  rl_beg_of_line(count, key);
}

void ctrl_e_handler(int count, int key) {
  rl_end_of_line(count, key);
}

void uparrow_handler()
{
    ptr=fopen("/home/sudeeksha/Documents/history.txt","a+");
    if(ptr == NULL){
      printf("File cannot be opened\n");
    }
    if(lineptr==1)
      return;

    char *line;
    size_t len = 0;
    int i = 1; 
    lineptr--;
    while(getline(&line, &len, ptr) != -1){  
      if(i == lineptr) 
      { 
        line[strlen(line)-1]='\0';
        break;  
      } 
      i++;
    } 
    rl_replace_line(line,0);
    ctrl_e_handler(0,0);
    fclose(ptr);
    return ;
}

void downarrow_handler()
{
  ptr=fopen("/home/sudeeksha/Documents/history.txt","a+");
  if(ptr == NULL){
    printf("File cannot be opened\n");
  }
  if(lineptr==lineno)
  {
    lineptr++;
    rl_replace_line("",0);
    return;
  }
  else if(lineptr>lineno)
  {
    rl_replace_line("",0);
    return;
  }

  char *line;
  size_t len = 0;
  int i = 1; 
  lineptr++; 
  while(getline(&line, &len, ptr) != -1){ 
    if(i == lineptr) 
    { 
      line[strlen(line)-1]='\0';
      break;  
    } 
    i++;
  }
  rl_replace_line(line,0);
  ctrl_e_handler(0,0);
  fclose(ptr);
  return ;
}

int main()
{
    char *str;
    char buf[10];
    nfp=fopen("/home/sudeeksha/Documents/numoflines.txt","r");
    if(nfp==NULL)
    {
        lineno=0;
    }
    else
    {
        size_t l=0;
        getline(&str,&l,nfp);
        str[strlen(str)]='\0';
        lineno = atoi(str);
        fclose(nfp);
    }
    char *input;
    lineptr=lineno;
    while (1)
    {
        rl_bind_key('\001', (rl_command_func_t *)ctrl_a_handler); // ctrl+A
        rl_bind_key('\005', (rl_command_func_t *)ctrl_e_handler); // ctrl+E
        rl_bind_keyseq("\\e[A", (rl_command_func_t *) uparrow_handler);
        rl_bind_keyseq("\\e[B", (rl_command_func_t *) downarrow_handler);

        sigaction(SIGINT, &(struct sigaction){.sa_handler = sigintHandler}, NULL);
        sigaction(SIGTSTP, &(struct sigaction){.sa_handler = sigstpHandler}, NULL);
        
        struct cmd *s;
        int n;
        bool background = false;
        bool pwd_cd_flag = false;
        input = readline("$ ");
        removeWhiteSpace(input);
        ptr=fopen("/home/sudeeksha/Documents/history.txt","a+");
        if(ptr == NULL){
          printf("File cannot be opened\n");
        }
        fputs(input,ptr);
        putc('\n',ptr);
        fflush(NULL);
        lineno++;
        lineptr=lineno+1;
        fclose(ptr);

        if (strcmp(input, "exit") == 0)
        {
            free(input);
            nfp=fopen("/home/sudeeksha/Documents/numoflines.txt","w");
            sprintf(buf,"%d",lineno);
            fputs(buf,nfp);
            fclose(nfp);
            exit(0);
        }
        s = parse_input(input, &n);
        int fd[n][2];
        for (int i = 0; i < n; i++)
        {
            if (pipe(fd[i]) == -1)
            {
                return 1;
            }
        }

        
        bool prev_out_to_a_file_flag = false;
        char *prev_out_to_a_file;
        background = false;
        pwd_cd_flag = false;
        for(int i=0;i<n;i++){
            if(s[i].bg==true){
                background=true;
                break;
            }
        }
        pno++;
        for (int i = 0; i < n; i++)
        {
            

            s[i].args[s[i].argc++] = NULL;

            int inputfd;
            int outputfd;
            if (s[i].bg == true)
            {
                background = true;
            }

            if (strcmp(s[i].args[0], "cd") == 0)
            {
                pwd_cd_flag = true;
                if (n == 1)
                {
                    if (s[i].argc > 3)
                    {
                        printf("Invalid input!\n");
                    }
                    else{
                        cd(s[i].args[1]);
                    }
                    continue;
                    
                }
                else
                {
                    printf("Error! cd command error\n");
                    break;
                }
            }
            else if (strcmp(s[i].args[0], "pwd") == 0)
            {
                pwd_cd_flag = true;
                if (s[i].argc > 2)
                {
                    printf("Invalid command!\n");
                }
                else{
                    char *cwd;
                    cwd = getcwd(NULL, 0);
                    printf("%s\n", cwd);
                }
                
                continue;
            }
            else if(strcmp(s[i].args[0], "delep") == 0){

                if(s[i].argc>3){
                    printf("Invalid command!\n");
                }
                else{
                    delep(s[i].args[1]);
                }
                continue; 
            }
            else if(strcmp(s[i].args[0], "sb") == 0){
                if(s[i].argc==3){
                    display_process_hierarchy(atoi(s[i].args[1]),0,0);
                }
                else if(s[i].argc==4 && strcmp(s[i].args[2],"-suggest")==0){
                    display_process_hierarchy(atoi(s[i].args[1]),0,1);
                }
                else{
                    printf("Invalid Command\n");
                }
                continue;
            }
            pid1 = fork();
            if (pid1 < 0)
            {
                return 2;
            }
            if (pid1 == 0)
            {

                setpgid(pid1,pno);
                grpid=getpgid(0);

                
                if (i == 0)
                {
                    // First command
                    if (i != n - 1)
                    {
                        
                        if (s[i].infile != NULL)
                        {
                            inputfd = open(s[i].infile, O_RDONLY);
                            dup2(inputfd, STDIN_FILENO);
                            close(inputfd);
                        }
                        if (s[i].outfile != NULL)
                        {
                            outputfd = open(s[i].outfile, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0666);
                            dup2(outputfd, STDOUT_FILENO);
                            close(outputfd);
                        }
                        else
                        {
                            dup2(fd[i][1], STDOUT_FILENO);
                        }
                    }
                    else
                    {
                        
                        if (s[i].infile != NULL)
                        {
                            inputfd = open(s[i].infile, O_RDONLY);
                            dup2(inputfd, STDIN_FILENO);
                            close(inputfd);
                        }
                        if (s[i].outfile != NULL)
                        {
                            outputfd = open(s[i].outfile, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0666);
                            dup2(outputfd, STDOUT_FILENO);
                            close(outputfd);
                        }
                    }
                }
                else if (i < n - 1)
                {
                    if (prev_out_to_a_file_flag == true)
                    {
                        inputfd = open(prev_out_to_a_file, O_RDONLY);
                        dup2(inputfd, STDIN_FILENO);
                        close(inputfd);
                    }
                    else
                    {
                        dup2(fd[i - 1][0], STDIN_FILENO);
                    }
                    if (s[i].outfile != NULL)
                    {
                        outputfd = open(s[i].outfile, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0666);
                        dup2(outputfd, STDOUT_FILENO);
                        close(outputfd);
                    }
                    else
                    {
                        dup2(fd[i][1], STDOUT_FILENO);
                    }
                }
                else
                {
                    if (prev_out_to_a_file_flag == true)
                    {
                        inputfd = open(prev_out_to_a_file, O_RDONLY);
                        dup2(inputfd, STDIN_FILENO);
                        close(inputfd);
                    }
                    else
                    {
                        dup2(fd[i - 1][0], STDIN_FILENO);
                    }
                    if (s[i].outfile != NULL)
                    {
                        outputfd = open(s[i].outfile, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0666);
                        dup2(outputfd, STDOUT_FILENO);
                        close(outputfd);
                    }
                }

                close(fd[i][0]);
                close(fd[i][1]);

                close(fd[i - 1][0]);
                close(fd[i - 1][1]);
                
                execute_command(s[i].argc,s[i].args);
                
            }

            if (s[i].outfile != NULL)
            {
                prev_out_to_a_file_flag = true;
                prev_out_to_a_file = s[i].outfile;
            }
           
            close(fd[i][1]);
            if(background==true){
                continue;
            }
            waitpid(pid1, NULL, 0);
            
        }
        
        for (int i = 0; i < n; i++)
        {
            close(fd[i][0]);
            close(fd[i][1]);
        }

        if (pwd_cd_flag || (background == true))
        {
            continue;
        }
        
        free(input);
        free(s);
    }
    return 0;

}