#include <iostream>
#include <vector>
#include <list>
#include <string>
#include <cmath>
#include <climits>
#include <ctime>
#include <cstdlib>
#include <fstream>
#include <cstring>
using namespace std;
static unsigned int dict_hash_function_seed=5381;
#define MAXBUCKET (1<<20)
//#define MAXMEMORY (4 * (1 << 10))
#define SAMPLE_COUNT 20
#define H 10
unsigned int MAXMEMORY [9] =  {2 * (1 << 20), 3 * (1 << 20), 4 * (1 << 20), 5 * (1 << 20), 6 * (1 << 20), 7 * (1 << 20), 8 * (1 << 20), 9 * (1 << 20), 10 * (1 << 20)};
struct KvItem
{
    string key;
    int len;
    long LRU;
    double k;
    long prev_LRU;
    double hot;
    struct KvItem* next;
};
struct Server
{
    long changed_max_memory;
    long cur_cap;
    long sampling_size;
    long all_access;
    long hit_access;
    long itemCount;
    long globalLRU;
};
double slope = 0.001;
Server server;
vector<KvItem *> redis(MAXBUCKET, NULL);
vector<bool> second_time(MAXBUCKET, false);
vector<unsigned int> random_bucket;

unsigned int getMurMurHashKey(string cur_key,int len)
{
    const void* key = (const void *)(cur_key.c_str());
	unsigned int seed=dict_hash_function_seed;
	const unsigned int m=0x5bd1e995;
	const int r=24;
	unsigned int h=seed ^ len;
	const unsigned char* data=(const unsigned char *)key;
	while(len>=4)
	{
		unsigned int k=*(unsigned int *)data;
		k*=m;
		k^=k>>r;
		k*=m;
		h*=m;
		h^=k;
		data+=4;
		len-=4;
	}
	switch(len)
	{
		case 3: h ^= data[2] << 16;
    	case 2: h ^= data[1] << 8;
    	case 1: h ^= data[0]; h *= m;
	}
	h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return (unsigned int)h;
}
void init_server(Server& server)
{
//    server.changed_max_memory = MAXMEMORY * (1 << 20);
    server.cur_cap = 0;
    server.sampling_size = 20;
    server.hit_access = 0;
    server.all_access = 0;
    server.itemCount = 0;
    server.globalLRU = 0;
}


int dictGetRandomKeys(KvItem** des, int sample_count)
{
    if(sample_count > server.itemCount)
        sample_count = server.itemCount;
    int stored = 0;
    //unsigned int bucket = rand() %MAXBUCKET;
    unsigned int cur_size = random_bucket.size();
    unsigned int rand_bucket = rand() % cur_size;
    unsigned int bucket = random_bucket[rand_bucket];
    while(stored <  SAMPLE_COUNT)
    {

        KvItem* de = redis[bucket];
        while(de)
        {
            *des = de;
            des++;
            de = de -> next;
            stored++;
            if(stored == sample_count)
            {
                return stored;
            }
        }
        swap(random_bucket[rand_bucket], random_bucket[cur_size - 1]);
        -- cur_size;
        rand_bucket = rand() % cur_size;
        //rand_bucket = (rand_bucket + 1) % random_bucket.size();
        bucket = random_bucket[rand_bucket];
        //bucket = (bucket + 1) %MAXBUCKET;
    }
    return stored;
}
bool dictDelete(string key)
{
    unsigned int bucket = getMurMurHashKey(key, key.size()) & (MAXBUCKET - 1);
    KvItem* de = redis[bucket];
    KvItem* prev = NULL;
    while(de)
    {
        if(de -> key == key)
        {
            if(!prev)
            {
                redis[bucket] = de -> next;
            }
            else
            {
                prev -> next = de -> next;
            }
            if(redis[bucket] == NULL)
            {
                second_time[bucket] = true;
            }
            server.itemCount -= 1;
            server.cur_cap -= (de -> len);
            delete de;
            return true;
        }
        prev = de;
        de = de -> next;
    }
    return false;
}
bool replacement(int samplingSize)
{
    KvItem* des[SAMPLE_COUNT + 1];
    int ret = dictGetRandomKeys(des, samplingSize);
    int minLRU = INT_MAX;
    KvItem* minItem = NULL;
    for(int i = 0; i < ret; ++i)
    {
        double prio = -1 * (des[i] ->k)/3.0 * server.globalLRU + des[i] -> hot;
        if(minLRU > prio)
        {
            minLRU = prio;
            minItem =  des[i];
        }
    }

    return dictDelete(minItem -> key);
}
KvItem* getKey(string key)
{
    unsigned int bucket = getMurMurHashKey(key, key.size()) & (MAXBUCKET - 1);
    KvItem* de = redis[bucket];
    while(de)
    {
        if(de -> key == key)
        {
            de -> k = 1.0 / (server.globalLRU - de -> LRU);
            double tmp = (de -> k / 3.0) - (de ->prev_LRU == 0 ? slope: (1.0 / ((de ->LRU - de ->prev_LRU)/3.0)));
            tmp *= server.globalLRU;
            de -> hot += (tmp + H);
            de -> prev_LRU = de -> LRU;
            de -> LRU = server.globalLRU;
            return de;
        }
        de = de -> next;
    }

    return NULL;

}
bool setKey(string key, int len, int index)
{
    KvItem *ret = NULL;
    unsigned int bucket = 0;
    ret = getKey(key);
    if(ret)
    {
        dictDelete(key);

    }
    KvItem* newItem = new KvItem;
    newItem -> key = key;
    newItem -> len = len;
    newItem -> k = slope;
    newItem -> hot = H;
    newItem -> prev_LRU = 0;
    newItem -> LRU = server.globalLRU;
    if(len > MAXMEMORY[index])
        return false;
    while(server.cur_cap + len > MAXMEMORY[index])
    {
        replacement(server.sampling_size);
    }
    bucket = getMurMurHashKey(key, key.size()) & (MAXBUCKET - 1);
    if(redis[bucket] == NULL && second_time[bucket] == false)
    {
        random_bucket.push_back(bucket);
    }
    newItem -> next = redis[bucket];
    redis[bucket] = newItem;
    server.cur_cap += len;
    ++(server.itemCount);
    return true;
}
void print()
{
    KvItem* vis = NULL;
    int cnt = 0;
    for(int i = 0; i < MAXBUCKET; ++i)
    {
        vis = redis[i];
        while(vis)
        {
            ++cnt;
            vis = vis -> next;
        }
    }
    cout<<"print:"<<cnt<<endl;
}
void clear()
{
    KvItem* tmp = NULL;
    for(int i = 0; i < MAXBUCKET; ++i)
    {
        while(redis[i])
        {
            tmp = redis[i];
            server.cur_cap -= tmp ->len;
            --server.itemCount;
            redis[i] = redis[i] -> next;
            free(tmp);
        }
    }
    random_bucket.clear();
    fill(second_time.begin(), second_time.end(), false);
}
int main()
{
	ios::sync_with_stdio(false);
	srand(time(NULL));
	string str;
	string str1;
	int c=0;
	char ch1[100];
	char ch2[500];
	int value1;
	int value2;
	ofstream fout;
    fout.open("testLinear.txt");
    for(int j = 7; j < 8; ++j){
	init_server(server);
        for(int i = 1; i <= 5; ++i){
        char number[50];
        sprintf(number, "%d", i);
        string filename = "C:\\Users\\hx\\ucloud\\trace_aet_170_" + string(number) + ".txt";
        //string filename = "/home/ucloud/ucloud/data/1102/trace_aet_170_" + string(number) + ".txt";
        fstream stream(filename.c_str());
        while(getline(stream,str))
        {
                cout<<str<<endl;
                sscanf(str.c_str(),"%s%s%d%d",ch1,ch2,&value1,&value2);
                //cout<<ch1<<","<<ch2<<","<<value1<<","<<value2<<endl;
                server.globalLRU += 1;
                if(string(ch1) == "GET")
                {
                        server.all_access++;
                        if(getKey(string(ch2)))
                        {
                                server.hit_access++;
                        }
                }
                else if(string(ch1) == "UPDATE")
                {
                        //if(strlen(ch2)==0)
                        //continue;
                        setKey(string(ch2),value2,j);
                }
        }
	stream.close();
        }
    fout<<"capacity: "<<(MAXMEMORY[j] >> 20) <<"M--------------------------------------"<<endl;
    fout<<"all size:"<<server.cur_cap<<endl;
    fout<<"all size:"<<server.itemCount<<endl;
	fout<<"hit rate:"<<double(server.hit_access) / server.all_access<<endl;
	fout<<"hit counts:"<<server.hit_access<<endl;
	cout<<endl;
	clear();
    }
	fout.close();
}

