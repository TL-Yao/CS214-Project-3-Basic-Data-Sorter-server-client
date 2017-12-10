#include "sorter_thread.h"


//if it is a number string, return 1, choosing int mergesort
//otherwise, choosing char mergesort
int isNum(char* string){
    int i = 0;
    int isNeg = 0;//0 if num is positive, 1 if num is negative
    int dec = 0; // 0 if num did not has a decimal, 1 otherwise i.e. 12.333.3
    
    if(string == NULL || strcmp(string, "") == 0){
        return 0;
    }
    //if the string start with '+' or '-' i.e. -123;
    if(string[0] == '+' || string[0] == '-'){
        i = 1;
    }
    
    while(i < strlen(string)){        
        if( !isdigit(string[i])){
            if(string[i] == '.' && !dec){ //i.e 1.3
                
                if(i == strlen(string) - 1 || !isdigit(string[i + 1])){//i.e 3.
                    return 0;
                }
    
                dec = 1;
                i++;
                continue;
            }
            return 0;
        }   
        i++;
    }
    return 1;
}    

int compare_num (char* c1, char* c2){
    float num_c1 = atof(c1);
    float num_c2 = atof(c2);
        
    if(num_c1 <= num_c2){
        return 0;
    }
    else{
        return 1;
    }
}

//if c1 < c2, return 0; if c1 = c2, return 0; otherwise, return 1;
int compare(char* c1, char* c2){
  int i = 0;
  int c1size = strlen(c1);
  int c2size = strlen(c2);

  //compare;
  if(!c1){
    return 0;
  }
  if(!c2 && c1){
      return 1;
  }
  if(isNum(c1) == 1 && isNum(c2) == 1){
      return compare_num(c1, c2);
  }
  else{
    if(strcasecmp(c1,c2) <= 0){
        return 0;
    }
    else{
        return 1;
    }
  }
}

/* This function is used to do mergeing. Merge L and R to Res; 
left is the size of L, right is the size of right; 
col is the column number we need to get; 
*/
void merge(row* L, row* R, row* res, int left, int right, int col){  

    int index = 0;
    int i = 0;
    int j = 0; 

    //compare L and R;
    while(i < left && j < right){
        if(compare(L[i].row_token[col], R[j].row_token[col]) == 0){
            res[index] = L[i];
            index++;
            i++;
        }else{
            res[index] = R[j];
            index++;
            j++;
        }
    }

    //when one of L and R is done, merge the remainder;
    while(i < left){
        res[index] = L[i];
        i++;
        index++;
    }
    while(j < right){
        res[index] = R[j];
        j++;
        index++;
    }
    i = 0;
    while(i < index){
        i++;
    }

}

void mergeSort(row* target, int col, int n){

    int i, mid;

    //base;
    if(n < 2){     
        return;
    }
    
    mid = n/2;

    //create the left part and right part of the row;
    row* L = (row*)malloc(mid * sizeof(row));   
    row* R = (row*)malloc((n-mid) * sizeof(row));

    //copy value to left and right row;
    for(i = 0; i < mid; i++){
        L[i] = target[i];
    } 
    for(i = mid; i < n; i++){
        R[i - mid] = target[i];
    }

    //recur;
    mergeSort(L, col, mid);
    mergeSort(R, col, n - mid);
    merge(L, R, target, mid, n - mid, col);

    free(L);
    free(R);

}
