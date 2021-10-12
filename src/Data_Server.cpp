#include "../inc/Data_Server.h"

using std::string;
using std::vector;
using std::shared_ptr;
using std::unique_ptr;
using std::mutex;
using std::unique_lock;
using std::find;

DataServer::DataServer(string directory, int sock, string metadata_server_ip,
        string metadata_server_port) : sockfd(sock), mu_p_vec(MAX_KEY_NUMBER), cache(1000000000), persistent(directory),
        CAS(shared_ptr<Cache>(&cache), shared_ptr<Persistent>(&persistent), shared_ptr<vector<unique_ptr<mutex>>>(&mu_p_vec)),
        ABD(shared_ptr<Cache>(&cache), shared_ptr<Persistent>(&persistent), shared_ptr<vector<unique_ptr<mutex>>>(&mu_p_vec)){

    this->metadata_server_port = metadata_server_port;
    this->metadata_server_ip = metadata_server_ip;

    DPRINTF(DEBUG_ABD_Server, "Creating mutexes for keys\n");
    for(int i = 0; i < MAX_KEY_NUMBER; i++){
        mu_p_vec[i].reset(new mutex);
    }
    DPRINTF(DEBUG_ABD_Server, "mutexes created\n");
}

// By wary while using datastore, since mutex is not applied
strVec DataServer::get_data(const string& key){
    const strVec* ptr = cache.get(key);
    if(ptr == nullptr){ // Data is not in cache
        strVec value = persistent.get(key);
        cache.put(key, value);
        return value;
    }
    else{ // data found in cache
        return *ptr;
    }
}

// By wary while using datastore, since mutex is not applied
int DataServer::put_data(const string& key, const strVec& value){
    cache.put(key, value);
    persistent.put(key, value);
    return S_OK;
}

string DataServer::put(const string& key, const string& timestamp, const string& value, const string& curr_class, uint32_t conf_id){

    if(curr_class == ABD_PROTOCOL_NAME){
        return ABD.put(key, conf_id, value, timestamp);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::get(const string& key, const string& curr_class, uint32_t conf_id){

    if(curr_class == ABD_PROTOCOL_NAME){
        return ABD.get(key, conf_id);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::get_timestamp(const string& key, const string& curr_class, uint32_t conf_id){

    if(curr_class == ABD_PROTOCOL_NAME){
        return ABD.get_timestamp(key, conf_id);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::reconfig_query(const string& key, const string& curr_class, uint32_t conf_id, uint32_t new_conf_id, const string& new_conf_placement){

    if(curr_class == ABD_PROTOCOL_NAME){
        return ABD.reconfig_query(key, conf_id, new_conf_id, new_conf_placement);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::reconfig_commit(const string& key, const string& timestamp, const string& value, const string& curr_class, uint32_t new_conf_id){

    if(curr_class == ABD_PROTOCOL_NAME){
        return ABD.reconfig_commit(key, timestamp, value, new_conf_id);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::finish_reconfig(const string &key, const string &curr_class, uint32_t conf_id){
    
    if(curr_class == ABD_PROTOCOL_NAME){
        return ABD.finish_reconfig(key, conf_id);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

int DataServer::getSocketDesc(){
    return sockfd;
}





// string DataServer::put_fin(const string& key, const string& timestamp, const string& curr_class, uint32_t conf_id){

//     check_block_keys(this->blocked_keys, key, curr_class, conf_id);

//     if(curr_class == CAS_PROTOCOL_NAME){
//         return CAS.put_fin(key, conf_id, timestamp);
//     }
//     else{
//         assert(false);
//     }
//     return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
// }

// string DataServer::reconfig_finalize(const string& key, const string& timestamp, const string& curr_class, uint32_t conf_id){

//     DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
//     add_block_keys(this->finished_reconfig_keys, key, curr_class, conf_id);
//     if(curr_class == CAS_PROTOCOL_NAME){
//         return CAS.get(key, conf_id, timestamp);
//     }else{
//         assert(false);
//     }
//     return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
// }

// bool DataServer::check_block_keys(std::vector<std::string>& blocked_keys, const std::string& key, 
//         const std::string& curr_class, uint32_t conf_id){
//     DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

//     // See if the key is in block mode
//     string con_key = construct_key(key, curr_class, conf_id);
//     if(find(blocked_keys.begin(), blocked_keys.end(), con_key) != blocked_keys.end()){
//         return true;
//     }

//     return false;
// }

// void DataServer::add_block_keys(std::vector<std::string>& blocked_keys, const std::string& key,
//             const std::string& curr_class, uint32_t conf_id){
//     DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

//     std::string con_key = construct_key(key, curr_class, conf_id);
//     assert(std::find(blocked_keys.begin(), blocked_keys.end(), con_key) == blocked_keys.end());
//     blocked_keys.push_back(con_key);

//     return;
// }

// void DataServer::remove_block_keys(std::vector<std::string>& blocked_keys, const std::string& key,
//             const std::string& curr_class, uint32_t conf_id){
//     DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

//     std::string con_key = construct_key(key, curr_class, conf_id);
//     auto it = std::find(blocked_keys.begin(), blocked_keys.end(), con_key);

//     if(it == blocked_keys.end()){
//         DPRINTF(DEBUG_RECONFIG_CONTROL, "WARN: it != this->blocked_keys.end() for con_key: %s\n", con_key.c_str());
//     }
//     if(it != blocked_keys.end()){
//         blocked_keys.erase(it);
//     }
//     return;
// }