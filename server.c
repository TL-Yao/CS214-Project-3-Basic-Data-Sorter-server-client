#include <stdio.h>
#include <string.h> 
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h> 
#include "sorter_thread.h"
row* data;
int is_fir = 0;
int size_data;
int count = 0;
int num_rows = -1;
int target_col = -1;
pthread_mutex_t lock;
pthread_mutex_t lock1;

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


void *sort(void* new_socket){
    FILE* fp = fopen("test.txt", "w");
    int* socketptr;
    socketptr = (int*)new_socket;
    int socket = *socketptr;
    int bufferSize = 10000;

    char buffer[bufferSize];
    char* fileData; 
    bzero(buffer, bufferSize);
    //get the number of row;
    read(socket, buffer, 4096);
    
    if(num_rows == -1){
        sscanf(buffer, "%d", &num_rows);
	printf("receive: %s\n", buffer);
    }
    bzero(buffer, bufferSize);
    

    //get the sort colume;
    read(socket, buffer, 2);
    if(target_col == -1){
        sscanf(buffer, "%d", &target_col);
	printf("receive: %s\n", buffer);
    }
    bzero(buffer, bufferSize);

    //get the total bytes;
    int totByte = 0;
    read(socket, buffer, 100);
    sscanf(buffer, "%d", &totByte);
	printf("receive: %s\n", buffer);
    bzero(buffer, bufferSize);
    

	//malloc or realloc for row* data; 
    pthread_mutex_lock(&lock1);
    if(is_fir == 0){
        data = (row*) malloc((num_rows + 1) * sizeof(row));
        
	    if(!data){
	        printf("fail to malloc data");
	        exit(1);
	    }

        fileData = (char*) malloc((totByte + 1) * sizeof(char));
        memset(fileData, 0, totByte + 1);
        if(!data){
	        printf("fail to malloc fileData");
	        exit(1);
	    }

        size_data = num_rows + 1;
        is_fir = 1;
    }else{
        row* dataptr;
        dataptr = (row*) realloc (data, sizeof(row) * (size_data + num_rows + 1));
        if(!dataptr){
	        printf("fail to realloc");
	        exit(1);
	    }
	    data = dataptr;
        size_data += num_rows + 1;
    }
    pthread_mutex_unlock(&lock1);

    //receive the rest of lines;
    int i = 0;
    int small = 0;
    while(totByte - i > bufferSize){
	small = 1;        
	read(socket, buffer, bufferSize);
	    printf("receive: %s\n", buffer);
	if(i == 0){
		strcpy(fileData, buffer);	
	}else{
		strcat(fileData, buffer);
	}
	    bzero(buffer, bufferSize);

        //store buffer to row* data;
        /*
        char** row_token = tokenizer(buffer, 28);

        pthread_mutex_lock(&lock);        
        data[count].row_token = row_token;
        data[count].row_text = (char*)malloc(strlen(buffer + 1));
        strcpy(data[count].row_text, buffer);

        count++; 
        pthread_mutex_unlock(&lock);
        */
        i += bufferSize;
    }

	read(socket, buffer, totByte - i);
	printf("receive: %s\n", buffer);
	fflush(stdout);
	if(small == 0){
		strcpy(fileData, buffer);
	} else {
		strcat(fileData,buffer);
	}	
	bzero(buffer, bufferSize);
}

void send_data(int sock){
    
    int sent; 
    
    //mergeSort(data, target_col, num_rows);
    
    //send the row number;
    char num_row[20];
    sprintf(num_row, "%d", count);
    sent =  write(sock, num_row, 20);
    
    if (sent < 0){
 		printf("Failed to send the row number\n");
    }
    
    //send the data;
    int i = 0;
    char buffer[2056];
    while(i < num_rows){
        //prepare for sending;
        strcpy(buffer, data[i].row_text);//copy data to buffer;
        buffer[strlen(buffer) - 1] = '\000';
        sent =  write(sock, buffer, 20);
        if (sent < 0){
 		    printf("Failed to send the row number\n");
        }
        bzero(buffer, 2056);
    }
}


int main(int argc, char** argv){
    
    if(pthread_mutex_init(&lock, NULL) != 0){
        printf("Error on lock.\n");
		exit(1);
    }

    if(pthread_mutex_init(&lock1, NULL) != 0){
        printf("Error on lock.\n");
		exit(1);
    }

    int socket_desc, new_socket, c;
    struct sockaddr_in server, client;
    char message[100];

    //create the socket;
    socket_desc = socket(AF_INET, SOCK_STREAM, 0);
    if(socket_desc == -1){
        printf("Failed to create socket. \n");
    }

    //initialize the sockaddr_in;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(8879);// depends on user input;

    //bind
    if(bind(socket_desc, (struct sockaddr*)&server, sizeof(server)) < 0){
        puts("bind falied\n");
    }

    //listen
    listen(socket_desc, 1024);

    //waiting for connection;
    puts("Waiting for incoming connections...");
    c = sizeof(struct sockaddr_in);

    int i = 0;
    struct sockaddr_storage srcaddr;
    socklen_t len;
    char ipstr[INET6_ADDRSTRLEN];
    int srcPort;
    pthread_t tid[1000];

    while(1){
        //accept the socket and get ipaddress and port
        new_socket = accept(socket_desc, (struct sockaddr *)&client, (socklen_t *)&c);
        len = sizeof(srcaddr);
        getpeername(new_socket, (struct sockaddr *)&srcaddr, &len);
        struct sockaddr_in *s = (struct sockaddr_in*) &srcaddr;
        srcPort = ntohs(s -> sin_port);
        inet_ntop(AF_INET, &s->sin_addr, ipstr, sizeof ipstr);
        printf("Client IP Address: (%s)\n", ipstr);
        printf("Client Port      : (%d)\n", srcPort);

        if (new_socket < 0)
        {
            perror("Accept failed");
            return 1;
        }
        puts("Connection accepted");

        //if signal is 's', it is sending file, if it is 'd', it finished sending files;
        char signal[2];
        read(new_socket, signal, 1);
        signal[strlen(signal)] = '\n';
        printf("receive: %s\n", signal);
        if(strcmp(signal, "s")){
    	    pthread_create(&tid[i], NULL, sort, &new_socket); //create the thread;
        }else if(strcmp(signal, "d")){

            //j is used to loop tid from 0 to i; i is the total number of tids.
            int j = 0; 
            while(j < i + 1){
                pthread_join(tid[j], NULL);
                j++;
            }
            //send_data(new_socket);
        } 
        bzero(signal, 2); 
        i++;      
    }

}
