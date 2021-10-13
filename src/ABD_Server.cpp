/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   ABD_Server.cpp
 * Author: shahrooz
 *
 * Created on January 4, 2020, 11:35 PM
 */

#include "../inc/ABD_Server.h"

using std::string;
using std::vector;
using std::lock_guard;
using std::mutex;

/*
 * KEY = key!ABD_PROTOCOL_NAME!conf_id
 * VALUE = value, timestamp, reconfig_in_progress, reconfig_completed, new_conf_id, new_conf_placement
 * We save KEY->VALUE in the storages
 *
 */

strVec ABD_Server::get_data(const std::string& key){
    const strVec* ptr = cache_p->get(key);
    if(ptr == nullptr){ // Data is not in cache
        strVec data = persistent_p->get(key);
        return data;
    }
    else{ // data found in cache
        return *ptr;
    }
}

int ABD_Server::put_data(const std::string& key, const strVec& value){
    cache_p->put(key, value);
    persistent_p->put(key, value);
    return S_OK;
}

ABD_Server::ABD_Server(const std::shared_ptr<Cache>& cache_p, const std::shared_ptr<Persistent>& persistent_p,
                       const std::shared_ptr<std::vector<std::unique_ptr<std::mutex>>>& mu_p_vec_p) : cache_p(cache_p),
                       persistent_p(persistent_p), mu_p_vec_p(mu_p_vec_p){
}

ABD_Server::~ABD_Server(){
}

int ABD_Server::init_key(const std::string& key, const uint32_t conf_id){
    
    DPRINTF(DEBUG_ABD_Server, "init_key started.\n");
    
    int ret = 0;
    string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id);
    
    vector <string> value{"init", "0-0", "f", "f", "", ""};
    put_data(con_key, value);
    
    return ret;
}

std::string ABD_Server::get_timestamp(const std::string& key, uint32_t conf_id){

    lock_guard<mutex> lock(*(mu_p_vec_p->at(stoui(key))));

    string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id); // Construct the unique id for the key
    DPRINTF(DEBUG_ABD_Server, "get_timestamp started and the key is %s\n", con_key.c_str());

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_ABD_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(), conf_id);
        init_key(key, conf_id);
        return DataTransfer::serialize({"OK", "0-0", "", ""});
    }

    if(data[3] == "t"){ // reconfig completed
        return DataTransfer::serialize({"OPFAIL", "", "", ""});
    }
    else if(data[2] == "t") { // reconfig in progress
        return DataTransfer::serialize({"OK", data[1], data[4], data[5]});
    } 
    else { // no reconfig
        return DataTransfer::serialize({"OK", data[1], "", ""});
    }
}

std::string ABD_Server::put(const std::string& key, uint32_t conf_id, const std::string& value, const std::string& timestamp){

    lock_guard<mutex> lock(*(mu_p_vec_p->at(stoui(key))));

    string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id);
    DPRINTF(DEBUG_ABD_Server, "put started and the key is %s\n", con_key.c_str());

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_ABD_Server, "put new con_key which is %s\n", con_key.c_str());
        vector <string> val{value, timestamp, "f", "f", "", ""};
        put_data(con_key, val);
        return DataTransfer::serialize({"OK", "", ""});
    }

    if(Timestamp(timestamp) > Timestamp(data[1])){
        data[0] = value;
        data[1] = timestamp;
        put_data(con_key, data);
    }

    if(data[2] == "t" || data[3] == "t"){ // Key reconfigured or in progress
        return DataTransfer::serialize({"OK", data[4], data[5]});
    }
    else{ 
        return DataTransfer::serialize({"OK", "", ""});
    }
}

std::string ABD_Server::get(const std::string& key, uint32_t conf_id){

    lock_guard<mutex> lock(*(mu_p_vec_p->at(stoui(key))));
    
    string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id); // Construct the unique id for the key
    DPRINTF(DEBUG_ABD_Server, "get started and the key is %s\n", con_key.c_str());

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_ABD_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(), conf_id);
        init_key(key, conf_id);
        return DataTransfer::serialize({"OK", "0-0", "init", "", ""});
    }


    if(data[3] == "t"){ // reconfig completed
        return DataTransfer::serialize({"OPFAIL", "", "", "", ""});
    }
    else if(data[2] == "t") { // reconfig in progress
        return DataTransfer::serialize({"OK", data[1], data[0], data[4], data[5]});
    }
    else{ 
        return DataTransfer::serialize({"OK", data[1], data[0], "", ""});
    }
}

std::string ABD_Server::reconfig_query(const std::string& key, uint32_t conf_id, uint32_t new_conf_id, const std::string& new_conf_placement){

    lock_guard<mutex> lock(*(mu_p_vec_p->at(stoui(key))));
    
    string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id); // Construct the unique id for the key
    DPRINTF(DEBUG_ABD_Server, "reconfig_query started and the key is %s\n", con_key.c_str());

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_ABD_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(), conf_id);
        init_key(key, conf_id);
        data = get_data(con_key);
    }

    data[2] = "t";
    data[4] = std::to_string(new_conf_id);
    data[5] = new_conf_placement;

    put_data(con_key, data);

    return DataTransfer::serialize({"OK", data[1], data[0]});
}

std::string ABD_Server::reconfig_commit(const std::string& key, const std::string& timestamp, const std::string& value, uint32_t new_conf_id){

    lock_guard<mutex> lock(*(mu_p_vec_p->at(stoui(key))));
    
    string con_key = construct_key(key, ABD_PROTOCOL_NAME, new_conf_id); // Construct the unique id for the key
    DPRINTF(DEBUG_ABD_Server, "reconfig_commit started and the key is %s\n", con_key.c_str());

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_ABD_Server, "reconfig_commit new con_key which is %s\n", con_key.c_str());
        vector <string> val{value, timestamp, "f", "f", "", ""};
        put_data(con_key, val);
        return DataTransfer::serialize({"OK"});
    }

    if(Timestamp(timestamp) > Timestamp(data[1])){
        data[0] = value;
        data[1] = timestamp;
        put_data(con_key, data);
    }

    return DataTransfer::serialize({"OK"});
}

std::string ABD_Server::finish_reconfig(const std::string &key, uint32_t conf_id){

    lock_guard<mutex> lock(*(mu_p_vec_p->at(stoui(key))));
    
    string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id); // Construct the unique id for the key
    DPRINTF(DEBUG_ABD_Server, "finish_reconfig started and the key is %s\n", con_key.c_str());

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_ABD_Server, "reconfig_commit new con_key which is %s\n", con_key.c_str());
        vector <string> val{"init", "0-0", "t", "t", "", ""};
        put_data(con_key, val);
        return DataTransfer::serialize({"OK"});
    }

    data[3] = "t";
    data[4] = ""; // to reduce size in cache, since we don't need to store nextConfig id
    data[5] = ""; // to reduce size in cache, since we don't need to store nextConfig placement info
    put_data(con_key, data);

    return DataTransfer::serialize({"OK"});
}
