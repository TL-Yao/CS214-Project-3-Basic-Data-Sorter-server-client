#include "sorter_thread.h"
#define MAX_FILE_DIR 256
int count = 0;
int database_count = 0;
int database_row_count = 0;
int size_database = 0;
pthread_t tid[256];
int tid_index = -1;
pthread_mutex_t lock;
pthread_mutex_t lock1;
row* database;
int isFir = FALSE;
row firstRow;

char* path_contact(const char* str1,const char* str2){ 
    char* result;  
    result=(char*)malloc(strlen(str1)+strlen(str2)+ 3);
    if(!result){
        printf("fail to allocate memory space\n");  
        exit(1);  
    }  

	strcpy(result,str1);
    strcat(result,"/");   
	strcat(result,str2);  
    return result;  
}  

char *trim(char *word, int index)
{
  char *end;
  while(isspace((unsigned char)*word)){
	word++;
  }
  if(*word == 0)
    return word;
  end = word + index;
  while(end > word && isspace((unsigned char)*end)) {
	  end--;
  }
  *(end+1) = 0;
  return word;
}


char** tokenizer(char* line, size_t num_col){
    
    int i, j, k;
    i = 0;//current position in line;
    j = 0;//current position in tmp;
    k = 0;//current position in result;
    
    char** result = (char**)malloc(sizeof(char*) * (num_col+1)); //return value;
	int m = 0;
	for(; m < num_col+1; m++){
		result[m] = (char*)malloc(sizeof(char) * 500);
	}
	int a;
    char* temp = (char*)malloc(500);//store each word;
	size_t start_quote = FALSE;//1 quote start, 0 quote end;
	
    //go through each character;
    while(i < strlen(line)){
		
		/*reach the start '"' */
		if(line[i] == '"' && start_quote == FALSE){
			start_quote = TRUE;
		}
				
		else if(line[i] == '"' && start_quote == TRUE){
			//store value in result
			//result[k] = (char*) malloc((j + 1) * sizeof(char));
			if(!result[k]){
				printf("malloc failed1\n");
				exit(1);
			}
			temp = trim(temp, j - 1);
			strncpy(result[k], temp, strlen(temp));
			memset(&temp[0], 0, strlen(temp));
			start_quote = FALSE;
			j = 0;
			k++;
			i++;
		}

		//split by ',' or reach the end of line;
        else if((line[i] == ',' || i == strlen(line) - 1) && start_quote != TRUE){
            //if there is no character; (eg: ,,)
            if(!temp){
                temp[0] = '\0';
			}
			if(i == strlen(line) - 1 && line[i] != '\n' && line[i] != ','){
				temp[j] = line[i];
				j++;
			}
            //store value to result;
			//result[k] = (char*)malloc((j+1) * sizeof(char));
			if(!result[k]){
				printf("malloc failed2 %d\n", num_col);
				exit(1);
			}
			temp = trim(temp, j - 1);			
			strncpy(result[k], temp, strlen(temp));
			memset(&temp[0], 0, strlen(temp));			
            j = 0;
			k++;
			//if the last character is ',';
			if(line[i] == ',' && i == strlen(line) - 1){
				temp[0] = '\0';
				//result[k] = (char*)malloc((j+1) * sizeof(char));
				if(!result[k]){
				printf("malloc failed3\n");
				exit(1);
				
				}
				strncpy(result[k], temp, strlen(temp));
				memset(&temp[0], 0, strlen(temp));								
			}

        }else{
			//copy character from line to temp;
			if(j == 0){
				if(line[i] == ' '){
					i++;
					continue;
				}				
			}
            temp[j] = line[i];
			j++;
		}
        i++;
	}
	i = 0;
    return result;
}

int tok_file(FILE *fp, row* data, int num_col){
	row rest_row;
	rest_row.row_text = (char*) malloc (sizeof(char) * BUF_SIZE);
	size_t curr_col = 0;
	size_t curr_row = 0; //current row number in row* data
	int i;//loop virable
	/*end of declaring variable*/
	
	/*deal with data*/
	i = 0;
	while(fgets(rest_row.row_text, BUF_SIZE-1, fp) != NULL){
		rest_row.row_len = strlen(rest_row.row_text);
		rest_row.row_token = (char**) malloc(sizeof(char *) * (num_col+1));
		rest_row.row_token = tokenizer(rest_row.row_text, num_col);
		data[curr_row++] = rest_row;		
	}
	return curr_row;
}

void sort(void* arg){
	/*open the parameter package*/
		struct sort_para* para;
		para = (struct sort_para*) arg;
		char* colname = para -> colname;
		char* tmppath = para -> tmppath;

		/*declare variable*/
		FILE *fp;
		fp = fopen(tmppath,"r");
		
		//first row:
		row first_row;
		row *data;
		char *token;
		char* target;
		size_t num_col = 1;
		int length;
		int i;	
		int j;
		int k;
		int num_row; 
		int target_col;
	
		/*split the 1st token by ','*/
		first_row.row_text = (char*) malloc (sizeof(char) * BUF_SIZE);		
		fgets(first_row.row_text, BUF_SIZE-1, fp);
		first_row.row_len = strlen(first_row.row_text);
		first_row.row_token = (char**) malloc(sizeof(char *) * first_row.row_len);
		for(i = 0; i < first_row.row_len; i++){
			first_row.row_token[i] = (char*)malloc(500);
		}
		i = 0;
		token = strtok(first_row.row_text, ",");
		first_row.row_token[0] = token;
	
		//split the rest of token in the first row
		while(token = strtok(NULL, ",")){
			first_row.row_token[num_col++] = token;	
		}
		first_row.num_col = num_col;

		
		//delete the '\n' in the last word;
		
		length = strlen(first_row.row_token[num_col - 1]);
		i = 1;
		while(first_row.row_token[num_col - 1][length - i] <= 13 && first_row.row_token[num_col - 1][length - i] >= 7){
			first_row.row_token[num_col - 1][length - i] = '\0';
			i++;
		}
        
		//trim blank space;
		i = 0;
		while(i < num_col){
			first_row.row_token[i] = trim(first_row.row_token[i], strlen(first_row.row_token[i]) - 1);
			i++;
		}
		
		//deal with rest rows;
		data = (row*) malloc (sizeof(row) * MAX_LINE);
		num_row = tok_file(fp, data, 28);
		
		//find the target column number;
			target = colname;
			target_col = 0;
			
			while(target_col < first_row.num_col){
				if(strcmp(first_row.row_token[target_col], target) == 0){
					break;
				}
				target_col++;
			}
			
			//no such title in the first row
			if(target_col == 28){
				printf("Wrong input, no such title.\n");
				fflush(stdout); 
				exit(1);
			}
			mergeSort(data, target_col, num_row);

			pthread_mutex_lock(&lock1);
			if(!isFir){
				isFir = TRUE;
				database = (row*) malloc ( sizeof(row) * (num_row + 1));
				size_database = num_row + 1;
				firstRow = first_row;
				/*type-in the rest row in current file*/
				i = 0;
				while(i < num_row){
					database[i] = data[i];
					database_row_count++;
					i++;
				}
			}
			else{
				/*deal with the rest file*/
				row* database_ptr;
				database_ptr = (row*) realloc (database, sizeof(row) * (size_database + num_row + 1));
				if(database_ptr){
					//printf("realloc!!!!");
					
				}
				size_database = size_database + num_row + 1;

				database = database_ptr;
				i = 0;
				while(i < num_row){
					database[database_row_count] = data[i];
					database_row_count++;
					i++;
				}
			}
			pthread_mutex_unlock(&lock1);
}

int isDirectory(char *path) {
    struct stat statbuf;
    if (stat(path, &statbuf) != 0)
        return 0;
    return S_ISDIR(statbuf.st_mode);
 }

int checkcsv(char* path, char* colname){
	FILE *fptr;
	fptr = fopen(path, "r");

	//get the first line of this file;
	char* text = (char*)malloc(500);
	fgets(text, BUF_SIZE-1, fptr);
	
	char* token = malloc(50);
	char** row_token = (char**) malloc(strlen(text) * sizeof(char*));
	int i;
	for(i = 0; i < strlen(text); i++){
		row_token[i] = (char*) malloc(strlen(text) * sizeof(char));
	}
	token = strtok(text, ",");
	row_token[0] = token;

	int num_col = 1;
	while(token = strtok(NULL, ",")){
		row_token[num_col++] = token;	
	}

    int length = strlen(row_token[num_col - 1]);
	i = 1;

	while(row_token[num_col - 1][length - i] <= 13 && row_token[num_col - 1][length - i] >= 7){
		row_token[num_col - 1][length - i] = '\0';
		i++;
	}

	//find the target column;
	int target_col = 0;
	while(target_col < 28){
		if(strcmp(row_token[target_col], colname) == 0){
			break;
		}
		target_col++;
	}

	//no such title, not the target csv file;
	if(target_col == 28){
		return 0;
	}
	return 1; 
	
}

void directory(void* arg){
	int i = 0;
	int err = 0;
	int dirnum = 0;
	int csv_num = 0;

    struct sort_para* para;
	para = (struct sort_para*) arg;
	char* colname = para -> colname;
	char* tmppath = para -> tmppath;
    char** dirpath = (char**)malloc(MAX_DIR*sizeof(char*));
	char** csv_path = (char**)malloc(MAX_DIR*sizeof(char*));
	struct sort_para** paraarr = (struct sort_para**)malloc(256*sizeof(struct sort_para*));
    struct sort_para** csv_arr = (struct sort_para**)malloc(256*sizeof(struct sort_para*));
	pthread_t* dirarr = (pthread_t*)calloc(256,sizeof(pthread_t));
	pthread_t* csvarr = (pthread_t*)calloc(256,sizeof(pthread_t));

	DIR *dir_p;
	dir_p = opendir(tmppath);
    struct dirent *dir_ptr;

    if(dir_p == NULL){
		printf("Wrong Path\n");
        exit(1);
    }
    
    
    // loop each file and folder in current directory
    while(dir_ptr = readdir(dir_p)){
        char* temppath;
        temppath = path_contact(tmppath, dir_ptr->d_name);
        
        /*skip forward and back folder*/
        if(!strcmp(dir_ptr->d_name, ".")  ||
		   !strcmp(dir_ptr->d_name, "..") ||
			dir_ptr->d_name[0] == '.'){//change
	            continue;
        }
        
        if(isDirectory(temppath)){
        
		    dirpath[dirnum] = malloc(strlen(temppath)+1);
            dirpath[dirnum] = strcpy(dirpath[dirnum], temppath);
            paraarr[dirnum] = (struct sort_para*)malloc(strlen(colname) + strlen(temppath) + 1);
            paraarr[dirnum] -> colname = colname;
            paraarr[dirnum] -> tmppath = temppath;
            dirnum++;

        }
        else{ // file
            char *name = dir_ptr->d_name;
            int length = strlen(name);
            /* .csv file*/
            if(name[length - 3] == 'c' &&
               name[length - 2] == 's' &&
               name[length - 1] == 'v' &&
			   checkcsv(temppath, colname)){

                   csv_path[csv_num] = malloc(strlen(temppath)+1);
				   csv_path[csv_num] = strcpy(csv_path[csv_num], temppath);
				   csv_arr[csv_num] = (struct sort_para*)malloc(strlen(colname) + strlen(temppath) + 1);
				   csv_arr[csv_num] -> colname = colname;
				   csv_arr[csv_num] -> tmppath = temppath;
				   csv_num++;	
            }
        }
        
    }

	/*create thread for directory*/
	
	for(i = 0; i < dirnum; i++){	
		err = pthread_create(&dirarr[i], NULL, (void *)&directory, (void*)paraarr[i]);
		if(err != 0){
            printf("Failed to create new thread.\n");
        }    
		printf("%d, ", dirarr[i]);
        
	}
	
	/*create thread for csv file*/
	int j;
	for(j = 0 ; j < csv_num; j++){  	
		err = pthread_create(&csvarr[j], NULL, (void *)&sort, (void*)csv_arr[j]);
		printf("%d, ", csvarr[j]);
		if(err != 0){
            printf("Failed to create new thread.\n");
        }  
	}
	
	/*join to wait all thread finish*/
	for (i = 0; i < dirnum; i++){
		pthread_join(dirarr[i], NULL);
	}
	for (i = 0; i < dirnum; i++){
		free(paraarr[i]);
	}
	for (j = 0; j < csv_num; j++){
		pthread_join(csvarr[j], NULL);
	}
	for (j = 0; j < csv_num; j++){
		free(csv_arr[j]);
	}
	pthread_mutex_lock(&lock);
		count += dirnum;
		count += csv_num;               
    pthread_mutex_unlock(&lock);
}

int main (int argc, char* argv[]){
	pthread_t self = pthread_self();
	printf("Initial PID: %d\nTIDS of all child threads: ", self);
	fflush(stdout);
	//declare variables;
    char* colname = (char*)malloc(100);
	char* dirname = (char*)malloc(500);
	char* odirname = (char*)malloc(500);
    char currDir[MAX_DIR];
    int c = FALSE;
	int d = FALSE;
	int o = FALSE;
	struct sort_para* para = (struct sort_para*) malloc (sizeof(struct sort_para*));

	if(argc < 2){
		printf("Too few input.\n");
		exit(0);
	}
	int i = 1;
	while(argv[i]){
		if(i > 6){
			printf("Too many input.\n");
			exit(0);
		}
		if(!strcmp(argv[i], "-c")){
			strcpy(colname, argv[i+1]);
			c = TRUE;
		}
		if(!strcmp(argv[i], "-d")){
			strcpy(dirname, argv[i+1]);
			d = TRUE;
		}
		if(!strcmp(argv[i], "-o")){
			strcpy(odirname, argv[i+1]);
			o = TRUE;
		}
		i += 2;
	}

	if(d == FALSE){
		dirname = getcwd(currDir, MAX_DIR);
	}

	if(o == FALSE){
		odirname = getcwd(currDir, MAX_DIR);
	}

	if(c == FALSE){
		printf("Wrong input, column name missed.\n");
		exit(1);
	}

	para -> colname = colname;
    para -> tmppath = dirname;
	para -> output_dir = odirname;

    if(pthread_mutex_init(&lock, NULL) != 0){
        printf("Error on lock.\n");
		exit(1);
    }

	if(pthread_mutex_init(&lock1, NULL) != 0){
        printf("Error on lock1.\n");
		exit(1);
    }
    
	pthread_t tmptid;
    int error = 0;
    error = pthread_create(&tmptid, NULL, (void*)directory, (void*)para);
    if(error != 0){
        printf("Failed to create thread.\n");
        pthread_exit(0);
    }

	void* end;
    pthread_join(tmptid, &end);
	if(isFir == TRUE){
		int target_col = 0;
		while(target_col < 28){
			if(strcmp(firstRow.row_token[target_col], colname) == 0){
				break;
			}				
			target_col++;
		}
		if(target_col == 28){
			printf("Wrong Input.\n");
			exit(1);
		}
		mergeSort(database, target_col, database_row_count);
		FILE* fp;
		char* fixname = "AllFiles-sorted-";
		char* csv = ".csv";
		char* filename = (char*)malloc(21 + strlen(colname));
		char* output_path;
		strncpy(filename, fixname, strlen(fixname));
		strcat(filename, colname);
		strcat(filename, csv);
		output_path = path_contact(odirname, filename);
		fp = fopen(output_path, "w");
		i = 0;
			while(i < 28){
				fprintf(fp, "%s",firstRow.row_token[i]);
				if(i != 27){
					fprintf(fp, ",");
				}else{
					fprintf(fp, "\n");
				}
				i++;
			}
		i = 0;
		int j = 0;
		int k = 0;
			next:
			while(i < database_row_count){
				while(j < 28){
					for(k = 0; k < strlen(database[i].row_token[j]); k++){
						if(database[i].row_token[j][k] == ',' && j != 27){
							fprintf(fp, "\"%s\",", database[i].row_token[j]);
							j++;
							break;
						}
						if(database[i].row_token[j][k] == ',' && j == 27){ 
							fprintf(fp, "\"%s\"", database[i].row_token[j]);
							i++;
							j = 0;
							goto next;
						}
					}
		
					fprintf(fp, "%s",database[i].row_token[j]);
					if(j != 27){
						fprintf(fp, ",");
					}else{ 
						fprintf(fp, "\n");
					}
					j++;
				}
				i++;
				j = 0;
			}
	}


	printf("\nTotal number of threads: %d\n", count + 1);
    pthread_mutex_destroy(&lock);
    pthread_mutex_destroy(&lock1);
	
    return 0;
}
