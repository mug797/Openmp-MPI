#include <iostream>
#include <omp.h>
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <cstdlib>
#include <cmath>

using namespace std;

class myPageRank {
    
	map<int,int> myMap;
    map <int, int> front_door;
    map <int, int> back_door;
    ifstream reader;
    vector<double> pr;
    vector<vector<double> > AdjacencyList;
    int N;
    int errorCnt;

    public:
    myPageRank (const char* fileName) : reader(fileName) {N=0;}

    void init () {
        string line;
        if (reader.is_open()) {
            while (getline (reader,line)) {
                char *cstr;
                cstr = const_cast<char*>(line.c_str());
                int a = strtol(cstr,&cstr, 10);
                if (myMap.find(a) == myMap.end()) {
                    myMap[a] = 1;
                    front_door[a] = N;
                    back_door[N] = a;
                    const vector<double> temp;
                    AdjacencyList.resize(front_door[a]+1);
                    AdjacencyList[front_door[a]] = temp;
                    N++;
                } else {
                    myMap[a] = myMap[a] + 1;
                }
                int b = strtol(cstr,&cstr, 10);
                if (myMap.find(b) == myMap.end()) {
                    front_door[b] = N;
                    back_door[N] = b;
                    const vector<double> temp;
                    AdjacencyList.resize(front_door[b]+1);
                    AdjacencyList[front_door[b]] = temp;
                    myMap[b] = 1;
                    N++;
                } else {
                    myMap[b] = myMap[b] + 1;
                }

                AdjacencyList[front_door[b]].resize(front_door[a]+1);
                AdjacencyList[front_door[a]].resize(front_door[b]+1);
                AdjacencyList[front_door[b]][front_door[a]] = 1;
                AdjacencyList[front_door[a]][front_door[b]] = 1;
            }
            reader.close();
        } 

        pr.resize(N);
        int block = N/15;

        omp_set_num_threads(15);

#pragma omp parallel 
        {
            #pragma omp for schedule(dynamic,block) nowait
            for (int i = 0; i < N; i++) {
                AdjacencyList[i].resize(N);
                vector <double> temp = AdjacencyList[i];
                pr[i] = 1/(double)N;
                for (int j = 0; j < N; j++) {
                    if (temp[j] == 1) 
                        temp[j] = 1/(double)(myMap[back_door[j]]);
                }
                AdjacencyList[i] = temp;
            }
        }

    }

    int iterate (double error) {
        
		errorCnt = 0;
        vector <double> new_pr(N);
        int block = N/15;

		#pragma omp parallel
        {
            #pragma omp for schedule(dynamic,block) nowait
            for (int i = 0; i < N; i++) {
                vector <double> temp = AdjacencyList[i];
                double sum = 0;
                for (int j = 0; j < N; j++) {
                    sum += temp[j]*pr[j];
                }
                new_pr[i] = 0.15*(1/(double)N) + 0.85*sum;
                if (fabs(new_pr[i] - pr[i]) > error) {
                    errorCnt++;
                }
            }
        }
        pr = new_pr;
        return errorCnt;
    }

    void printAdj (const char* fileName) {
        ofstream myfile (fileName);
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
	     //   myfile << AdjacencyList[i][j] << " ";
	    	}
	    	myfile << cout;
		}
    }

    void printPr (const char* fileName) {
        ofstream myfile (fileName);
        for (int i = 0; i < N; i++) {
            myfile << i << " " << pr[i]  << endl;
        }
    }
};

int main () { 

    myPageRank* pr = new myPageRank("facebook_combined.txt");

    pr->init();
    int count;
    int iter = 0;
    //pr->printAdj("Adj");
    do {
        count = pr->iterate(0.00001);
        iter++;
		//if (iter == 1) 
			//pr->printPr("abc");
        
		cout << count << endl;
    } while (count > 0);
    pr->printPr("Output_Task1.txt");
    cout << iter << endl;
    return 1;
}

