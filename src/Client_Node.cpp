/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Client_Node.cpp
 * Author: shahrooz
 * 
 * Created on August 30, 2020, 5:37 AM
 */

#include "../inc/Client_Node.h"

Client_Node::Client_Node(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts,
        uint32_t metadata_server_timeout, uint32_t timeout_per_request, std::vector<DC*>& datacenters){

    cas = new CAS_Client(id, local_datacenter_id, retry_attempts, metadata_server_timeout, timeout_per_request,
            datacenters, this);
    abd = new ABD_Client(id, local_datacenter_id, retry_attempts, metadata_server_timeout, timeout_per_request,
            datacenters, this);
}

Client_Node::~Client_Node(){
    if(abd != nullptr){
        delete abd;
        abd = nullptr;
    }
    if(cas != nullptr){
        delete cas;
        cas = nullptr;
    }
}

const uint32_t& Client_Node::get_id() const{
    if(abd != nullptr){
        return abd->get_id();
    }
    if(cas != nullptr){
        return cas->get_id();
    }
    static uint32_t ret = -1;
    return ret;
}

int Client_Node::update_placement(const std::string& key){
    
    int ret = 0;

    std::string ready_conf_id;
    Placement ready_placement;
    std::string toret_conf_id;
    Placement toret_placement;

    if(this->abd != nullptr){
        ret = ask_metadata(this->abd->get_metadata_server_ip(), this->abd->get_metadata_server_port(), key,
                           ready_conf_id, ready_placement, toret_conf_id, toret_placement, this->abd->get_retry_attempts(),
                           this->abd->get_metadata_server_timeout());
    }
    else{
        ret = -1;
        return ret;
    }
    
    assert(ret == 0);

    DPRINTF(DEBUG_CAS_Client, "ask_metadata fetched ready_conf_id=%s toret_conf_id=%s\n", ready_conf_id.c_str(), toret_conf_id.c_str());

    keys_info[key].first.confid = ready_conf_id;
    keys_info[key].first.placement = ready_placement;
    keys_info[key].second.confid = toret_conf_id;
    keys_info[key].second.placement = toret_placement;

    assert(ready_placement.m != 0);

    DPRINTF(DEBUG_CAS_Client, "finished\n");
    return ret;
}

const std::pair<Configuration, Configuration>& Client_Node::get_placement(const std::string& key, const bool force_update){

    uint64_t le_init = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    
    if(force_update){
        assert(update_placement(key) == 0);
        auto it = this->keys_info.find(key);
        DPRINTF(DEBUG_CAS_Client, "latencies: %lu\n", time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
        return it->second;
    }
    else{
        auto it = this->keys_info.find(key);
        if(it != this->keys_info.end()){
            DPRINTF(DEBUG_CAS_Client, "latencies: %lu\n", time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
            return it->second;
        }
        else{
            assert(update_placement(key) == 0);
            DPRINTF(DEBUG_CAS_Client, "latencies: %lu\n", time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
            return this->keys_info[key];
        }
    }
}

// Ready/Active configuration for the key known by Client
uint32_t Client_Node::get_conf_id(const std::string& key){
    auto it = this->keys_info.find(key);
    if(it != this->keys_info.end()){
        return stoui(it->second.first.confid);
    }
    else{
        assert(update_placement(key) == 0);
        return stoui(this->keys_info[key].first.confid);
    }
}

int Client_Node::put(const std::string& key, const std::string& value){
    int retval = this->abd->put(key, value);

    if(retval == S_RECFG) {
        this->abd->put(key, value);
    }

    return retval;
}


int Client_Node::get(const std::string& key, std::string& value){
    int retval = this->abd->get(key, value);

    if(retval == S_RECFG) {
        this->abd->get(key, value);
    }

    return retval;
}
