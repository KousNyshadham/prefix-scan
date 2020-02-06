#include <vector>
#include <fstream>
#include <iostream>
#include <string>
#include <pthread.h>
#include <math.h>
#include <iomanip>
#include <chrono>     

using namespace std::chrono; 

struct kous_barrier{
    int nThreads;
    int count;
    bool phase;
    pthread_mutex_t lock;
    pthread_cond_t all_here;
};

std::string outputPath;
bool custom;
int numThreads;
std::vector<int> globalInt;
std::vector<std::vector<float> > globalFloat;
int padding;
pthread_barrier_t barrier;
kous_barrier bestbarrier;

void kous_barrier_init(kous_barrier *bestbarrier, void *attr, int nThreads){
    bestbarrier->nThreads = nThreads;
    bestbarrier->count = 0;
    bestbarrier->bool = 0;
    pthread_mutex_init(&bestbarrier->lock, NULL);
    pthread_cond_init(&bestbarrier->all_here, NULL);
}

void kous_barrier_wait(kous_barrier *bestbarrier){
    pthread_mutex_lock(&bestbarrier->lock);
    bestbarrier->count++;
    if(bestbarrier->count == bestbarrier->nThreads){
        bestbarrier->count = 0;
        bestbarrier->bool = ~bool;
        pthread_cond_broadcast(&bestbarrier->all_here);
        pthread_mutex_unlock(&bestbarrier->lock);
    }
    else{
        int oldphase = bestbarrier->phase;
        while(oldphase ==bestbarrier->phase){
            pthread_cond_wait(&bestbarrier->all_here, &bestbarrier->lock);
        }
        pthread_mutex_unlock(&bestbarrier->lock);
    }
}

struct addops{
    int l;
    int r;
};

template <typename T>
T scan_op(T x, T y){
    return x + y;
}

template <typename T>
std::vector<T> scan_op(std::vector<T> x, std::vector<T> y){
    for(int i = 0; i < x.size(); i++){
        x[i] = x[i] + y[i];
    }
    return x;
}

void* add(void* arg){
    struct addops *addop = (struct addops*) arg;
    int l = addop->l;
    int r = addop->r;
    for(int d = 0; d < log2 (globalInt.size()-1); d++){
        int dpow = (int)pow(2,d);
        int dpow1 = (int)pow(2,d+1);
        for(int i = l; i < r; i+=dpow1){
            if(i%dpow1!= 0){continue;}
            globalInt[i + dpow1-1] = scan_op(globalInt[i+dpow-1], globalInt[i+dpow1-1]);
        }
        if(custom){
            kous_barrier_wait(&bestbarrier);
        }
        else{
            pthread_barrier_wait(&barrier);
        }
    }
}

void* down(void* arg){
    struct addops *downop = (struct addops*) arg;
    int l = downop->l;
    int r = downop->r;
    for(int d = (int)log2(globalInt.size()-1); d>=0; d--){
        int dpow = (int)pow(2,d);
        int dpow1 = (int)pow(2,d+1);
        for(int i = l; i< r; i+=dpow1){
            if(i%dpow1!=0){continue;}
            int temp = globalInt[i+dpow-1];
            globalInt[i+dpow-1] = globalInt[i+dpow1-1];
            globalInt[i+dpow1-1] = scan_op(temp,globalInt[i+dpow1-1]);
        }
        if(custom){
            kous_barrier_wait(&bestbarrier);
        }
        else{
            pthread_barrier_wait(&barrier);
        }
    }
}

void* downFloat(void* arg){
    struct addops *downop = (struct addops*) arg;
    int l = downop->l;
    int r = downop->r;
    for(int d = (int)log2(globalFloat.size()-1); d>=0; d--){
        int dpow = (int)pow(2,d);
        int dpow1 = (int)pow(2,d+1);
        for(int i = l; i< r; i+=dpow1){
            if(i%dpow1!=0){continue;}
            std::vector<float> temp = globalFloat[i+dpow-1];
            globalFloat[i+dpow-1] = globalFloat[i+dpow1-1];
            globalFloat[i+dpow1-1] = scan_op(temp, globalFloat[i+dpow1-1]);
        }
        if(custom){
            kous_barrier_wait(&bestbarrier);
        }
        else{
            pthread_barrier_wait(&barrier);
        }
    }
}

void* addFloat(void* arg){
    struct addops *addop = (struct addops*) arg;
    int l = addop->l;
    int r = addop->r;
    for(int d = 0; d < log2 (globalFloat.size()-1); d++){
        int dpow = (int)pow(2,d);
        int dpow1 = (int)pow(2,d+1);
        for(int i = l; i < r; i+=dpow1){
            if(i%dpow1!= 0){continue;}
            globalFloat[i + dpow1-1] = scan_op(globalFloat[i+dpow-1], globalFloat[i+dpow1-1]);
        }
        if(custom){
            kous_barrier_wait(&bestbarrier);
        }
        else{
            pthread_barrier_wait(&barrier);
        }
    }
}

template <typename T>
void outputPrefixSum(std::vector<T> prefixSum){
    std::ofstream outputFile;
    outputFile.open(outputPath);
    for(int i = 0; i < prefixSum.size()-padding; i++){
        if(i == prefixSum.size()-padding-1){
            outputFile << prefixSum[i];
            break;
        }
        outputFile << prefixSum[i] << std::endl;
    }
}

template <typename T>
void outputPrefixSum(std::vector<std::vector<T> > prefixSum){
    std::ofstream outputFile;
    outputFile.open(outputPath);
    outputFile << std::fixed << std::setprecision(4);
    for(int i = 0; i < prefixSum.size()-padding; i++){
       for(int j = 0; j < prefixSum[0].size()-1; j++){
           outputFile << prefixSum[i][j] << ",";
       } 
       if(i == prefixSum.size() - padding - 1){
           outputFile << prefixSum[i][prefixSum[0].size()-1];
           break;
       }
       outputFile << prefixSum[i][prefixSum[0].size()-1] << std::endl;
    }
}

void sequentialPrefixScan(std::vector<int> arr){
    std::vector<int> prefixSum(arr.size());
    prefixSum[0] = 0;
    for(int i = 1; i < arr.size(); i++){
        prefixSum[i] = scan_op(prefixSum[i-1], arr[i]);
    }
    globalInt = prefixSum;
}

void sequentialPrefixScan(std::vector<std::vector<float> > arr){
    std::vector<std::vector<float> > prefixSum(arr.size());
    std::vector<float> empty(arr[0].size());
    prefixSum[0] = empty;
    for(int i = 1; i < arr.size(); i++){
        prefixSum[i] = scan_op(prefixSum[i-1], arr[i]);
    }
    globalFloat = prefixSum;
}

void parallelPrefixScan(std::vector<int> arr){
    globalInt = arr;
    int newNumThreads = numThreads > arr.size()/2 ? arr.size()/2: numThreads;// (int)log2(numThreads);
    while(log2(newNumThreads) != (int) log2(newNumThreads)){
        newNumThreads-=1;
    }
    int n = arr.size();
    int createThreads = newNumThreads;
    int leftOverThreads = numThreads - createThreads;
    pthread_t tids[numThreads];
    if(custom){
        kous_barrier_init(&bestbarrier, NULL, numThreads);
    }
    else{
        pthread_barrier_init(&barrier,NULL,numThreads);
    }
    struct addops addop[numThreads];
    for(int i = 0; i < numThreads; i++){
        if(i >= createThreads){
            addop[i].l = 1;
            addop[i].r = 0;
            pthread_create(&tids[i], NULL, add, &addop[i]);
            continue;
        }
        addop[i].l = i * (n/createThreads);
        addop[i].r = i * (n/createThreads) + n/createThreads;
        pthread_create(&tids[i], NULL, add, &addop[i]);
    }
    for(int i = 0; i < numThreads; i++){
        pthread_join(tids[i], NULL);
    }
    globalInt[globalInt.size()-1] = 0;
    for(int i = 0; i < numThreads; i++){
        pthread_create(&tids[i], NULL, down, &addop[i]);
    }
    for(int i = 0; i < numThreads; i++){
        pthread_join(tids[i], NULL);
    }
}

void parallelPrefixScan(std::vector<std::vector<float> > arr){
    globalFloat = arr;
    int newNumThreads = numThreads > arr.size()/2 ? arr.size()/2: numThreads;// (int)log2(numThreads);
    while(log2(newNumThreads) != (int) log2(newNumThreads)){
        newNumThreads-=1;
    }
    int n = arr.size();
    int createThreads = newNumThreads;
    int leftOverThreads = numThreads - createThreads;
    pthread_t tids[numThreads];
    if(custom){
        kous_barrier_init(&bestbarrier, NULL, numThreads);
    }
    else{
        pthread_barrier_init(&barrier,NULL,numThreads);
    }
    struct addops addop[numThreads];
    for(int i = 0; i < numThreads; i++){
        if(i >= createThreads){
            addop[i].l = 1;
            addop[i].r = 0;
            pthread_create(&tids[i], NULL, addFloat, &addop[i]);
            continue;
        }
        addop[i].l = i * (n/createThreads);
        addop[i].r = i * (n/createThreads) + n/createThreads;
        pthread_create(&tids[i], NULL, addFloat, &addop[i]);
    }
    for(int i = 0; i < numThreads; i++){
        pthread_join(tids[i], NULL);
    }
    std::vector<float> empty(globalFloat[0].size());
    globalFloat[globalFloat.size()-1] = empty;
    for(int i = 0; i < numThreads; i++){
        pthread_create(&tids[i], NULL, downFloat, &addop[i]);
    }
    for(int i = 0; i < numThreads; i++){
        pthread_join(tids[i], NULL);
    }
}

int main(int argc, char* argv[]){
    std::string inputPath;
    custom = argc == 8 ? true : false;
    for(int i = 1; i < argc; i++){
        std::string arg = std::string(argv[i]);
        if(arg == "-n"){
            numThreads = std::stoi(argv[++i]);
        }
        if(arg == "-i"){
            inputPath = argv[++i]; 
        }
        if(arg == "-o"){
            outputPath = argv[++i];
        }
    }
    std::ifstream inputFile;
    inputFile.open(inputPath);
    std::string stringDim;
    std::getline(inputFile, stringDim);
    int dim = std::stoi(stringDim);
    std::string stringN;
    std::getline(inputFile, stringN);
    int n = std::stoi(stringN);
    std::vector<int> onedim;
    std::vector<std::vector<float> > multidim;
    for(int i = 0; i < n; i++){
        std::string line;
        std:getline(inputFile,line);
        if(dim == 0 || dim == 1){
            onedim.push_back(std::stoi(line));
        }
        else{
            std::vector<float> vector;
            std::string value = "";
            for(int i = 0; i < line.size(); i++){
                if(line[i] == ','){
                    vector.push_back(std::stod(value));
                    value = "";
                }
                else{
                    value = value + line[i];
                }
            }
            if(value != ""){
                vector.push_back(std::stod(value));
            }
            multidim.push_back(vector);
        }
    }
    padding = 0;
    while(log2(n) != (int) log2(n)){
        n++;
        padding++;
    }
    if(multidim.size() == 0){
        for(int i = 0; i < padding; i++){
            onedim.push_back(0);
        }
    }
    if(onedim.size() == 0){
        for(int i = 0; i < padding; i++){
            std::vector<float> empty(multidim[0].size());
            multidim.push_back(empty);
        }
    }
    if(numThreads == 0){
        if(onedim.size() == 0){
            auto start = high_resolution_clock::now(); 
            sequentialPrefixScan(multidim);
            auto stop = high_resolution_clock::now(); 
            auto duration = duration_cast<seconds>(stop - start); 
            std::cout << duration.count() << std::endl; 
            outputPrefixSum(globalFloat);

        }
        else{
            auto start = high_resolution_clock::now(); 
            sequentialPrefixScan(onedim);
            auto stop = high_resolution_clock::now(); 
            auto duration = duration_cast<seconds>(stop - start); 
            std::cout << duration.count() << std::endl; 
            outputPrefixSum(globalInt);
        }
    }
    else{
        if(onedim.size() == 0){
            auto start = high_resolution_clock::now(); 
            parallelPrefixScan(multidim);
            auto stop = high_resolution_clock::now(); 
            auto duration = duration_cast<seconds>(stop - start); 
            std::cout << duration.count() << std::endl; 
            outputPrefixSum(globalFloat);
        }
        else{
            auto start = high_resolution_clock::now(); 
            parallelPrefixScan(onedim);
            auto stop = high_resolution_clock::now(); 
            auto duration = duration_cast<seconds>(stop - start); 
            std::cout << duration.count() << std::endl; 
            outputPrefixSum(globalInt);
        }
    }
}