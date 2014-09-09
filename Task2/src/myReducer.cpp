#include <mpi.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <map>
#include <cstdlib>
#include <iterator>
#include <cmath>

using namespace std;

//read the input file

void ReadFile() {
	ifstream infile("100000_key-value_pairs.csv");
	int a=0, b=0;
	char ch;
	int chunkSize=0;
	int count = 0;
	string line;

	if (infile.is_open()) {
		cout << "Success" << endl;
	} else {
		cout << "Error opening the file" << endl; 
	}

 	getline(infile, line);
 	while (infile >> a >> ch >> b) {
		count++;
	//   	cout<< a <<"	"<< b <<endl;
 	}

 	cout<< "number of key value pairs: "<<count<<endl; 
	//go to the beginning of the file
	infile.clear();
	infile.seekg(0, ios::beg);

  	int size=0;
  	MPI_Comm_size(MPI_COMM_WORLD, &size);

 	chunkSize = ceil((float)count/(size-1));
 	cout<<"chunk size: "<< chunkSize<<endl;
 	// chunkSize = count;
	//int buf_size = 2*(chunkSize-1) ;
	// cout << "chunkSize: "<<chunkSize<<endl;
 	int data[2*chunkSize];
 	count = 0;
	// cout << "My chunk size = " << 2*chunkSize << endl;

 	getline(infile, line);
 	int dest = 1;
 	while (infile >> a >> ch >> b) {
		data[count++] = a;
		data[count++] = b;
		if(count == 2*chunkSize) {
			MPI_Send(data, count, MPI_INT, dest++, 0, MPI_COMM_WORLD);
			count = 0;
		}
 	}

  if (count > 0) {
	MPI_Send(data, count, MPI_INT, dest++, 0, MPI_COMM_WORLD);
  }
}

//------------------------------------------------------------------
void receive() {
	int count = 0;
	MPI_Status status;

  	int world_rank;
  	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

	MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
	MPI_Get_count(&status, MPI_INT, &count);
	int rcv_data[count];

	cout << "Got from master at rank " << world_rank << " " << count << endl;
  	MPI_Recv(rcv_data, count, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	map <int, int> input;
	map <int, map <int, int> > dataProcess;
	int a,b;	
	
	cout << "All count " << count << endl;
	for(int i = 0; i< count; i = i+2) {
		a = rcv_data[i];
		b = rcv_data[i+1];
		input[a] += b;
	}

	cout << "Map 50 " << input[50] << " from rank " << world_rank << endl;
  	int size = 0;
  	MPI_Comm_size(MPI_COMM_WORLD, &size);

	int key, processID, keyProcess;
	typedef map<int, int> ::iterator it_type;
	int counting = 0;
	for(it_type iterator = input.begin(); iterator != input.end(); ++iterator) {
		key = iterator->first;
		keyProcess = (key%(size - 1))+1;


		if (key == 50) 
			cout << "Big one 5032 mapped to " << keyProcess << endl;
		dataProcess[keyProcess][key] = iterator->second;
		counting++;
	}

	cout << "MY stuff here only " << counting <<endl;

	MPI_Request reqsSent[size];
	typedef map<int, map<int, int> > ::iterator it_type_map;

	for(int i = 1; i < size; i++) {
		int processNo = i;
		map <int, int> valMap = dataProcess[i];
		
		if(processNo == world_rank) { 
			//cout<<"in if for Rank: "<<world_rank<<endl; 
			continue; 			
		}
		
		int dataSize = valMap.size();
		int* dataToSend = (int*)malloc(2*sizeof(int)*dataSize);
		i = 0;
		int dest = 1;
		for(it_type iterator =valMap.begin(); iterator != valMap.end(); ++iterator) {
		
			if (iterator->first == 50) 
				cout << "!!! Sending 50 from " <<world_rank << " with " << iterator->second << " to " <<processNo <<" boundary ? " << i <<  endl;
			dataToSend[i++] = iterator->first;
			dataToSend[i++] = iterator->second;
		}
		MPI_Isend(dataToSend, 2*dataSize, MPI_INT, processNo, 1, MPI_COMM_WORLD, &reqsSent[processNo]);
	}

// send the data to other processes
	map <int, int> final_result = dataProcess[world_rank];
	int rcv_count = 0;
	MPI_Status rcv_status;
	int flag;
	int flags[size];
	int flagTracker[size];
	int numSetFlags = 0;
	MPI_Request reqsRecv[size];
	map<int, int*> received_data;
	map<int, int> received_sizes;
		
	for(int i = 1; i < size; i++) {
		flags[i]=0;
		flagTracker[i] = 0;
	}


	flags[0] = 1;
	flagTracker[0] = 1;

	flags[world_rank] = 1;
	flagTracker[world_rank] = 1;

	while(numSetFlags < size-2) {	
		for(int i = 1; i < size; i++) {
			if(i == world_rank) { 
				continue;
			}	
			
			if (flags[i] != 1) 
				MPI_Iprobe (i, 1, MPI_COMM_WORLD, &flags[i], &rcv_status);
			if(flags[i] == 1 && flagTracker[i] == 0 ) {
				flagTracker[i] = 1;
				numSetFlags++;

				MPI_Get_count(&rcv_status, MPI_INT, &rcv_count);	
				int* receive_data = (int*)malloc(sizeof(int)*rcv_count); //on heap 
				received_data[i] = receive_data;
				received_sizes[i] = rcv_count;
				//cout<<"rcv_count: "<<rcv_count<<" for rank no: "<<world_rank<<endl;
				MPI_Irecv (receive_data, rcv_count, MPI_INT, i, 1, MPI_COMM_WORLD, &reqsRecv[i]); 	
			}

		}
	}

	for(int i = 1; i < size; i++) {
		flags[i]=0;
	}

	numSetFlags = 0;
	while(numSetFlags < size-2) {
		for(int i = 1; i<size; i++ ) {
			if (i == world_rank) {
				continue;
			}

			if(flags[i] == 0) {
				MPI_Test(&reqsRecv[i], &flags[i], &rcv_status);
				//flags[i]=1;
				if (flags[i] == 1) {
					numSetFlags++;
					int key, value;	
					int *myData = received_data[i];			
					
					for(int j = 0; j < received_sizes[i];) {
						key = myData[j++];
						value = myData[j++]; 
						//if (key == 50) 
							//cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!5032 at " << world_rank << " with " << value << endl;
						final_result[key] += value;
						}
					}
			} else {
				continue;
			}
		}
	}


//print received data--------------------------------------------------

	typedef map<int, int> ::iterator it_type_dataChunk;

	    for(it_type_dataChunk iterator = final_result.begin(); iterator != final_result.end(); ++iterator){
			if (iterator->first == 4296)
				cout<<" dhantenantan " << iterator->first<<"	"<<iterator->second<<endl;
		}
//---------------------------------------------------------------

	int final_data[2*final_result.size()];

	typedef map<int, int> ::iterator it_type_dataChunk;
	int j = 0;
	cout << "Total from rank " << world_rank << " " << final_result.size() << endl; 
	for(it_type_dataChunk iterator = final_result.begin(); iterator != final_result.end(); ++iterator){
		final_data[j] = iterator->first;
		final_data[j+1] = iterator->second;
		j = j+2;
	}

	MPI_Send(final_data, j, MPI_INT, 0, 3, MPI_COMM_WORLD);

}


int main(int argc, char** argv) {

	MPI_Init(NULL, NULL);

  	// Get the number of processes
  	int world_size;
  	MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  	// Get the rank of the process
  	int world_rank;
  	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  	// Get the name of the processor
  	char processor_name[MPI_MAX_PROCESSOR_NAME];
  	int name_len;
  	MPI_Get_processor_name(processor_name, &name_len);

	map <int, int> answer;

	if (world_rank == 0) {
	  	ReadFile();
		MPI_Status status;	
		int count;
		for(int i = 1; i<world_size; i++) {
			MPI_Probe(i, 3, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_INT, &count);
	        int rcv_data[count];
	 		MPI_Recv(rcv_data, count, MPI_INT, i, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			int key, value;
			for(int j = 0; j<count;) {
				key = rcv_data[j++];
				value = rcv_data[j++];
				answer[key] = value;	
			}
		
		//cout<<"received data from process: "<<i<<endl;	
			ofstream myfile("Output_Task2.txt");

			typedef map<int, int> ::iterator it_type_final;
			int key1, value1;
			cout << "Number of keys at master " << answer.size() << endl;
			for(it_type_final iterator = answer.begin(); iterator != answer.end(); ++iterator){
				key1 = iterator -> first;
				value1 = iterator -> second;
				myfile << key1 <<"\t"<< value1<<endl;
			//	cout<<key1<<"	"<<value1<<endl;
			}
		}
	//	cout<<"after read file"<<endl;
  	} else {
		receive ();
		//cout<<"after receive"<<endl;
  	}

	//cout<<"after read"<<endl;

  // Finalize the MPI environment. No more MPI calls can be made after this
  	MPI_Finalize();
 	return 0;
}
