#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<ctype.h>
#include<dirent.h>
#include<pthread.h>
#include<unistd.h>
#include<sys/syscall.h>
#include<sys/socket.h>
#include<arpa/inet.h>
#include<sys/time.h>
#include"sorter.h"

#define PATH_LENGTH 256
#define MAX_CHARS 2056
#define COLS 30

char sortCol[256];
char socketAddr[256];
int port;

int checkCmdInpts(int argNum, char *argCmds[], char *tDir, char *pDir){
	if(!(argNum == 7 || argNum == 9 || argNum == 11)){ return -1; }

	if(strcmp(argCmds[1], "-c") != 0 
	|| strcmp(argCmds[3], "-h") != 0
	|| strcmp(argCmds[5], "-p") != 0)
	{
		return -1;
	}

	if(findCols(argCmds[2]) == -1){ return -1; }
	strcpy(sortCol, argCmds[2]);
	strcpy(socketAddr, argCmds[4]); 
	
	//Check Port is Number
	int length = strlen(argCmds[6]);
	int j;
	for(j = 0; j < length; j++){
		if(!isdigit(argCmds[6][j])){
			return -1;
		}
	}
	sscanf(argCmds[6], "%d", &port);

	//See if -d or -o options are used.
	int d, dPos = -1, o, oPos = -1, i;
	if(argNum >= 9){
		for(i = 9, d = 0, i = 0; i <= argNum; i += 2){
			if(strcmp(argCmds[i], "-d") == 0){ d++; dPos = i + 1; }
			else if(strcmp(argCmds[i], "-o") == 0){ o++; oPos = i + 1; }
		}
		if(argNum == 9){ if (d + o != 1) { return -1; }}
		if(argNum == 11){ if( d != 1 && o != 1) { return -1; }}
	}

	//Set directory and output paths
	if(dPos != -1){
		if(!checkDir(argCmds[dPos])){ return -1; } 
		else { strcpy(tDir, argCmds[dPos]); }
	} else { getCurrentDir(tDir); }
	if(tDir[strlen(tDir) - 1] != '/'){ strcat(tDir, "/"); }
	
	if(oPos != -1){
		if(!makeDir(argCmds[oPos])){ return -1; }
		else { 
			strcpy(pDir, argCmds[oPos]); 
		}
	} else if (dPos != -1){ strcpy(pDir, tDir);
	} else { getCurrentDir(pDir); }
	if(pDir[strlen(pDir) - 1] != '/'){ strcat(pDir, "/"); }
	
	return 1;
}

int findCols(char *colName){
	char *col[] = {
		"color", 
		"director_name",
		"num_critic_for_reviews",
		"duration",
		"director_facebook_likes",
		"actor_3_facebook_likes",
		"actor_2_name",
		"actor_1_facebook_likes",
		"gross",
		"genres",
		"actor_1_name",
		"movie_title",
		"num_voted_users",
		"cast_total_facebook_likes",
		"actor_3_name",
		"facenumber_in_poster",
		"plot_keywords",
		"movie_imdb_link",
		"num_user_for_reviews",
		"language",
		"country",
		"content_rating",
		"budget",
		"title_year",
		"actor_2_facebook_likes",
		"imdb_score",
		"aspect_ratio",
		"movie_facebook_likes"
	};
	
	int i;
	for (i = 0; i < 28; i++){
		if(strcmp(col[i], colName) == 0){return i;}
	}

	return -1;
}

int checkDir(char *dirName){
	DIR *directory = opendir(dirName);
	if(directory == NULL){
		closedir(directory);
		return 0;
	}
	closedir(directory);
	return 1;	
}

int makeDir(char *dirName){
	DIR *directory = opendir(dirName);
	if(directory == NULL){
		if(mkdir(dirName, 0777) == -1){
			closedir(directory);
			return 0;
		}
	}
	closedir(directory);
	return 1;
}

void getCurrentDir(char *dest){
	char cwd[1024];
	getcwd(cwd, sizeof(cwd));
	strcpy(dest, cwd);
	strcat(dest,"/");
}

void *traverseDirectories(void *void_folder){
	int csvNum = 0, dirNum = 0, csvSpace = 256, dirSpace = 256;
	char **csvList = (char **) calloc(csvSpace, sizeof(char *));
	char **dirList = (char **) calloc(dirSpace, sizeof(char *));
	
	int tid = syscall(SYS_gettid);
	char *folder = (char *) void_folder;
	DIR *targetDirectory = opendir(folder);
	if(targetDirectory == NULL){ exit(1); }
	struct dirent *dirFile;

	//Iterate through all folder members. See what's in the list
	while((dirFile = readdir(targetDirectory)) != NULL){
		int length = strlen(dirFile-> d_name);
		if(length >= 5){
			//See if CSV file
			char *ext = (char *) malloc (sizeof(char) * 5);
			ext[0] = dirFile-> d_name[length - 4];
			ext[1] = dirFile-> d_name[length - 3];
			ext[2] = dirFile-> d_name[length - 2];
			ext[3] = dirFile-> d_name[length - 1];
			ext[4] = '\000';

			if(strcmp(ext, ".csv") == 0){ 
				if(csvNum == csvSpace){
					csvSpace *= 2;
					csvList = (char **) realloc(csvList, sizeof(char *) * (csvSpace));
				}
				csvList[csvNum++] = dirFile-> d_name;
			}
			free(ext);
		} 
	
		if(dirFile-> d_type & DT_DIR){
			if((strcmp(dirFile-> d_name, "..") != 0) && (strcmp(dirFile-> d_name , ".") != 0)){
				if(dirNum == dirSpace){
					dirSpace *= 2;
					dirList = (char **) realloc(dirList, sizeof(char *) * (dirSpace));
				}
				dirList[dirNum++] = dirFile-> d_name;
			}
		}
	}

	pthread_t *csv_tid_listing = (pthread_t *) calloc(4096, sizeof(pthread_t));
	pthread_t *dir_tid_listing = (pthread_t *) calloc(4096, sizeof(pthread_t));
	if(csvNum > 0){ csv_tid_listing = makeCSVThreads(csvNum, csvList, folder, csv_tid_listing); }
	if(dirNum > 0){ dir_tid_listing = makeDIRThreads(dirNum, dirList, folder, dir_tid_listing); }

	int i;
	void *result;
	for(i = 0; i < csvNum; i++){ pthread_join(csv_tid_listing[i], &result); }
	for(i = 0; i < dirNum; i++){ pthread_join(dir_tid_listing[i], &result); }
	free(csvList);
	free(dirList);
	free(csv_tid_listing);
	free(dir_tid_listing);
	free(folder);
	
	if(tid != getpid()){ pthread_exit(NULL); }
}

pthread_t *makeCSVThreads(int num, char **csvList, char *folder, pthread_t list[]){
	int i;
	for(i = 0; i < num; i++){
		char *csvPath = (char *) calloc(1, PATH_LENGTH);
		strcpy(csvPath, folder);
		if(folder[strlen(folder) - 1] != '/'){ strcat(csvPath, "/"); }
		strcat(csvPath, csvList[i]);
		pthread_t id;
		pthread_create(&id, NULL, sortDriver, csvPath);
		list[i] = id;
	}
	return list;
}

pthread_t *makeDIRThreads(int num, char **dirList, char *folder, pthread_t list[]){
	int i;
	for(i = 0; i < num; i++){
		char *targetDirectory = (char *) calloc(1, PATH_LENGTH);
		strcpy(targetDirectory, folder);
		if(folder[strlen(folder) - 1] != '/'){ strcat(targetDirectory, "/"); }
		strcat(targetDirectory, dirList[i]);
		pthread_t id;
		pthread_create(&id, NULL, traverseDirectories, targetDirectory);
		list[i] = id;
	}
	return list;
}

void *sortDriver(void *path){
	char *csvName = (char *) path;
	int *lineCount = (int *) calloc(1, sizeof(int));
	char **rawData = (char **) calloc(1, sizeof(char *));
	
	//See if empty CSV
	rawData = loadFile(rawData, lineCount, csvName);
	if(*lineCount <= 0){
		free(csvName);
		free(lineCount);
		free(rawData);
		pthread_exit(NULL);
	}

	//See if correct headers in CSV
	int hasValidHeader = 0;
	hasValidHeader = checkHeader(rawData);

	//Make Socket
	int sock;
	struct sockaddr_in server;
	char message[1000], server_reply[2000];
	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0) { printf("Could not create socket"); }
	puts("Socket created");
	bzero(&server, sizeof(server));
	//server.sin_addr.s_addr = inet_addr(socketAddr);
	server.sin_addr.s_addr = inet_addr("127.0.0.1");
	server.sin_family = AF_INET;
	//server.sin_port = htons(port);
	server.sin_port = htons(8885);

	//Connect to remote server
	while (connect(sock, (struct sockaddr *)&server, sizeof(server)) < 0)
	{
	    printf("Attempting to Connect with %s\n", csvName);
	    sleep(2);
	}

	printf("Connected with %s\n", csvName);
	
	//Send Line Count
	char buffer[4096];
	char lineSend[200];
	snprintf(lineSend, sizeof(lineSend), "%d", *lineCount);
	int sent;
	sent = write(sock, lineSend, 4096);
	if(sent < 0){ printf("Failed to send line count"); pthread_exit(NULL); }
	
	//Send Sort Column
	sent = write(sock, sortCol, 4096);
	if(sent < 0){ printf("Failed to send sort column"); pthread_exit(NULL); }

	//Send all lines
	int line = 1;
	while(line < *lineCount){
	    	char buffer[2056];
		bzero(buffer, 2056);
		strcpy(buffer, rawData[line]);
		printf("Preparing to send: %s", buffer);
		buffer[strlen(buffer) - 1] = '\000';
		sent = write(sock, buffer, 4096);
		if (sent < 0)
		{
			printf("Failed to send line: %d", line);
			printf("Line was: %s", buffer);
		}
		bzero(buffer, 4096);
	}

	//Receive a reply from the server
	puts("Server reply :");
	char resp[10000];
	int len;
	struct timeval tv;
	tv.tv_sec = 5; 
	tv.tv_usec = 0;
	setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));

	while (1)
	{
	    len = read(sock, resp, 9999);
	    if (len <= 0) { break; }
	    printf("%s\n",resp);
	}

	close(sock);
	free(csvName);
	free(lineCount);
	pthread_exit(NULL);	
}

//Start storing all lines into an array of row structs
char **loadFile(char **arr, int *lineCount, char *fileName){
	//Grab a line from the CSV File
	char *tempLine = (char *) calloc(MAX_CHARS, sizeof(char));
	int i = 0;

	FILE *file = fopen(fileName, "r");
	while(fgets(tempLine, MAX_CHARS, file)){
		//This part is unnecessary, but will save space.
		char* trueLine = (char *) calloc(strlen(tempLine), sizeof(char) + 1);
		strcpy(trueLine,  tempLine);

		arr[i++] = trueLine;
		arr = (char**) realloc(arr, sizeof(char *) * (i + 1));
	}

	*lineCount = i;
	free(tempLine);
	fclose(file);
	return arr;
}

int checkHeader(char **data){
	int i;
	char **fieldArr;
	//Setup a record
	fieldArr = (char **) calloc(COLS, sizeof(char *));
		
	//Copy contents over so we can work on and edit new string
	char *tempLine = (char *) calloc(MAX_CHARS, sizeof(char));
	strcpy(tempLine, data[i]);

	//Get specific fields from data
	int j, k = 0, commaCount = 0;
	for(j = 0; j < COLS; j++){
		//Too many columns, bad file.
		if(j == 28){ return 0; }

		char *field = (char *) calloc(MAX_CHARS, sizeof(char));
		int l;
		if(tempLine[k] != '\000'){
			for(l = 0; tempLine[k] != ',' && tempLine[k] != '\n'; k++, l++){
				//String Qualifier - Ignore them and store everything until '"' is found
				if(tempLine[k] == '"'){
					k++;
					while(tempLine[k] != '"'){ field[l++] = tempLine[k++]; }
					k++;
					break;
				} else if(tempLine[k] != '\r'){ field[l] = tempLine[k]; }
			}
			k++;
		}

		//Check for leading and ending white spaces and non alphanumerics-- Only applicable for nonempty fields
		if(strlen(field) != 0){
			//Trim ending white spaces.
			if(field[strlen(field) - 1] == ' ' || field[strlen(field) - 1] < 32 || field[strlen(field) - 1] > 126){
				int m = strlen(field) - 1;
				while((field[m] == ' ' || field[m] < 32 || field[m] > 126) && m != 0){ field[m--] = '\000'; }
			}
			//Trim any beginning white space
			if(field[0] == ' '){
				int m = 0, n = 0;
				while(field[m] == ' ' || field[m] < 32 || field[m] > 126){ m++;	}
					while(m <= strlen(field)){ field[n++] = field[m++]; }
			}
		}

		field = (char *) realloc(field, strlen(field) + 1);
		fieldArr[j] = field; 
	}
	
	free(tempLine);
	//Check all headers match in order
	return findHeader(fieldArr);
}

int findHeader(char *line, char **headers){
	char *col[] = {
		"color", 
		"director_name",
		"num_critic_for_reviews",
		"duration",
		"director_facebook_likes",
		"actor_3_facebook_likes",
		"actor_2_name",
		"actor_1_facebook_likes",
		"gross",
		"genres",
		"actor_1_name",
		"movie_title",
		"num_voted_users",
		"cast_total_facebook_likes",
		"actor_3_name",
		"facenumber_in_poster",
		"plot_keywords",
		"movie_imdb_link",
		"num_user_for_reviews",
		"language",
		"country",
		"content_rating",
		"budget",
		"title_year",
		"actor_2_facebook_likes",
		"imdb_score",
		"aspect_ratio",
		"movie_facebook_likes"
	};

	int i;
	for(i = 0; i < COLS; i++){
		if(strcmp(line, headers[i]) != 0){
			return 0;
		}
	}
	return 1;
}

int main(int argc, char *argv[]){
	//Setup and extract command line arguments.
	char *targetDirectory = (char *) calloc(1, PATH_LENGTH);
	char *printDirectory = (char *) calloc(1, PATH_LENGTH);
	//Global variables
	bzero(sortCol, 256);
	bzero(socketAddr, 256);
	port = 0;
	if(checkCmdInpts(argc, argv, targetDirectory, printDirectory) == -1){ exit(0); }


	//Establish connection to server


	//Recursive traversal of folders. This function leads to the socket transfer part.
	traverseDirectories(targetDirectory);
	free(printDirectory);


	return 0;
}

