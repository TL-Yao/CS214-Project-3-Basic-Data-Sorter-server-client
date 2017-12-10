#ifndef SORTER_H
#define SORTER_H

#define BUF_SIZE 1025
#define MAX_LINE 10000
#define TRUE 1
#define FALSE 0
#define MAX_DIR 4097

#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <dirent.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include<pthread.h>
#define TRUE 1
#define FALSE 0

struct sort_para{
    char* colname;
    char* tmppath;
    char* output_dir;
};

typedef struct{
    char** row_token;
    char * row_text;
    size_t row_len;
    size_t num_col;
    int comma;//true if there is no comma, false if there is a comma in the row
}row;

//mergesort.c

int compare(char* c1, char* c2);
void merge(row* L, row* R, row* res, int left, int right, int col);
void mergeSort(row* target, int col, int n);

#endif