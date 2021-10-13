/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   Reconfig.cpp
 * Author: shahrooz
 *
 * Created on March 23, 2020, 6:34 PM
 */

#include "../inc/Reconfig.h"
#include <algorithm>
#include <future>
#include <unordered_set>
#include "gbuffer.pb.h"

using std::max;
using std::string;
using std::vector;
using std::move;
using std::to_string;
using std::unique_ptr;

namespace CAS_helper_recon{

    void _send_one_server(const string operation, promise <strVec>&& prm, const  string key,
                          const Server server, const string current_class, const uint32_t conf_id, const string value = "",
                          const string timestamp = ""){

        DPRINTF(DEBUG_CAS_Client, "started.\n");
        EASY_LOG_INIT_M(string("to do ") + operation + " on key " + key + " with conf_id " + to_string(conf_id), false);

        strVec data;
        Connect c(server.ip, server.port);
        if(!c.is_connected()){
            prm.set_value(move(data));
            return;
        }

        EASY_LOG_M(string("Connected to ") + server.ip + ":" + to_string(server.port));

        data.push_back(operation); // get_timestamp, put, put_fin, get
        data.push_back(key);
        if(operation == "reconfig_write" || operation == "finish_reconfig"){
            if(value.empty()){
                DPRINTF(DEBUG_CAS_Client, "WARNING!!! SENDING EMPTY STRING TO SERVER.\n");
            }
            data.push_back(timestamp);
            data.push_back(value);
        }
        else if(operation == "reconfig_finalize"){
            data.push_back(timestamp);
        }
        else if(operation == "reconfig_query"){

        }
        else{
            assert(false);
        }

        data.push_back(current_class);
        data.push_back(to_string(conf_id));

        DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

        EASY_LOG_M("request sent");

        data.clear();
        string recvd;
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data = DataTransfer::deserialize(recvd);
            EASY_LOG_M(string("response received with status: ") + data[0]);
            prm.set_value(move(data));
        }
        else{
            data.clear();
            EASY_LOG_M("response failed");
            prm.set_value(move(data));
        }

        DPRINTF(DEBUG_CAS_Client, "finished successfully. with port: %u\n", server.port);
        return;
    }

    inline uint32_t number_of_received_responses(vector<bool>& done){
        int ret = 0;
        for(auto it = done.begin(); it != done.end(); it++){
            if(*it){
                ret++;
            }
        }
        return ret;
    }

    /* This function will be used for all communication.
     * datacenters just have the information for servers
     */
    int failure_support_optimized(const string& operation, const string& key, const string& timestamp, const vector<string>& values, uint32_t RAs,
                                  uint32_t number_to_respond, vector<uint32_t> servers, uint32_t total_num_servers, vector<DC*>& datacenters,
                                  const string current_class, const uint32_t conf_id, uint32_t timeout_per_request, vector<strVec> &ret){
        DPRINTF(DEBUG_CAS_Client, "started.\n");
        EASY_LOG_INIT_M(string("to do ") + operation + " on key " + key + " with quorum size " + to_string(number_to_respond), DEBUG_CAS_Client);

        map <uint32_t, future<strVec> > responses; // server_id, future
        vector<bool> done(total_num_servers, false);
        ret.clear();

        int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
        RAs--;
        for (auto it = servers.begin(); it != servers.end(); it++) { // request to all servers
            promise <strVec> prm;
            responses.emplace(*it, prm.get_future());
            thread(&_send_one_server, operation, move(prm), key, *(datacenters[*it]->servers[0]),
                        current_class, conf_id, values[*it], timestamp).detach();
        }

        EASY_LOG_M("requests were sent to Quorum");

        chrono::system_clock::time_point end = chrono::system_clock::now() +
                                                    chrono::milliseconds(timeout_per_request);
        auto it = responses.begin();
        while (true){
            if (done[it->first]){
                it++;
                if(it == responses.end())
                    it = responses.begin();
                continue;
            }

            if(it->second.valid() &&
               it->second.wait_for(chrono::milliseconds(1)) == future_status::ready){
                strVec data = it->second.get();
                if(data.size() != 0){
                    ret.push_back(data);
                    done[it->first] = true;
                    if(number_of_received_responses(done) == number_to_respond){
                        EASY_LOG_M("Responses collected successfully");
                        op_status = 0;
                        break;
                    }
                }
            }

            if(chrono::system_clock::now() > end){
                // Access all the servers and wait for Q1.size() of them.
                EASY_LOG_M("Responses collected, FAILURE");
                op_status = -1; // You should access all the server.
                break;
            }

            it++;
            if(it == responses.end())
                it = responses.begin();
            continue;
        }

        return op_status;
    }
}

void serializePlacementToString(const Placement& new_conf_placement, string& new_conf_placement_str) {
    packet::Placement p;

    p.set_protocol(new_conf_placement.protocol);
    p.set_m(new_conf_placement.m);
    p.set_k(new_conf_placement.k);
    p.set_f(new_conf_placement.f);

    for(auto s: new_conf_placement.servers){
        p.add_servers(s);
    }

    for(auto& quo: new_conf_placement.quorums){
        packet::Quorums* quorums = p.add_quorums();
        for(auto q : quo.Q1){
            quorums->add_q1(q);
        }
        for(auto q : quo.Q2){
            quorums->add_q2(q);
        }
    }

    p.SerializeToString(&new_conf_placement_str);
}

namespace ABD_helper_recon{
    inline uint32_t number_of_received_responses(vector<bool>& done){
        int ret = 0;
        for(auto it = done.begin(); it != done.end(); it++){
            if(*it){
                ret++;
            }
        }
        return ret;
    }

    void _send_one_server(const string operation, promise <strVec>&& prm, const string key,
                          const Server server, const string current_class, const uint32_t conf_id, const string value,
                          const string timestamp, const uint32_t new_conf_id, const Placement& new_conf_placement){

        DPRINTF(DEBUG_ABD_Server, "started.\n");
        EASY_LOG_INIT_M(string("to do ") + operation + " on key " + key + " with conf_id " + to_string(conf_id), false);

        strVec data;
        Connect c(server.ip, server.port);
        if(!c.is_connected()){
            prm.set_value(move(data));
            return;
        }

        EASY_LOG_M(string("Connected to ") + server.ip + ":" + to_string(server.port));

        data.push_back(operation); // reconfig_query, reconfig_commit, finish_reconfig
        data.push_back(key);
        if(operation == "reconfig_commit"){
            if(value.empty()){
                DPRINTF(DEBUG_ABD_Server, "WARNING!!! SENDING EMPTY STRING TO SERVER.\n");
            }
            data.push_back(timestamp);
            data.push_back(value);
        }

        data.push_back(current_class);
        data.push_back(to_string(conf_id));

        if(operation == "reconfig_query"){
            data.push_back(to_string(new_conf_id));
            
            string new_conf_placement_str;
            serializePlacementToString(new_conf_placement, new_conf_placement_str);
            data.push_back(new_conf_placement_str);
        }
        else if(operation == "reconfig_commit" || operation == "finish_reconfig"){
        }
        else{
            assert(false);
        }

        DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

        EASY_LOG_M("request sent");

        data.clear();
        string recvd;
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data = DataTransfer::deserialize(recvd);
            EASY_LOG_M(string("response received with status: ") + data[0]);
            prm.set_value(move(data));
        }
        else{
            data.clear();
            EASY_LOG_M("response failed");
            prm.set_value(move(data));
        }

        DPRINTF(DEBUG_CAS_Client, "finished successfully. with port: %u\n", server.port);
        return;
    }

    /* This function will be used for all communication.
     * datacenters just have the information for servers
     */
    int failure_support_optimized(const string& operation, const string& key, const string& timestamp, const string& value, uint32_t RAs,
                                  uint32_t number_to_respond, vector<uint32_t> servers, uint32_t total_num_servers, vector<DC*>& datacenters,
                                  const string current_class, const uint32_t conf_id, const uint32_t new_conf_id, const Placement& new_conf_placement,
                                  uint32_t timeout_per_request, vector<strVec> &ret){

        DPRINTF(DEBUG_CAS_Client, "started.\n");
        EASY_LOG_INIT_M(string("to do ") + operation + " on key " + key + " with quorum size " + to_string(number_to_respond), DEBUG_ABD_Client);

        map <uint32_t, future<strVec> > responses; // server_id, future
        vector<bool> done(total_num_servers, false);
        ret.clear();

        int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
        for (auto it = servers.begin(); it != servers.end(); it++) { // request to all servers
            promise <strVec> prm;
            responses.emplace(*it, prm.get_future());
            thread(&_send_one_server, operation, move(prm), key, *(datacenters[*it]->servers[0]),
                        current_class, conf_id, value, timestamp, new_conf_id, new_conf_placement).detach();
        }

        EASY_LOG_M("requests were sent to Quorum");

        chrono::system_clock::time_point end = chrono::system_clock::now() +
                chrono::milliseconds(timeout_per_request);
        auto it = responses.begin();
        while (true){
            if (done[it->first]){
                it++;
                if(it == responses.end())
                    it = responses.begin();
                continue;
            }

            if(it->second.valid() &&
               it->second.wait_for(chrono::milliseconds(1)) == future_status::ready){
                strVec data = it->second.get();
                if(data.size() != 0){
                    ret.push_back(data);
                    done[it->first] = true;
                    if(number_of_received_responses(done) == number_to_respond){
                        EASY_LOG_M("Responses collected successfully");
                        op_status = 0;
                        break;
                    }
                }
            }

            if(chrono::system_clock::now() > end){
                // Access all the servers and wait for Q1.size() of them.
                EASY_LOG_M("Responses collected, FAILURE");
                op_status = -1; // You should access all the server.
                break;
            }

            it++;
            if(it == responses.end())
                it = responses.begin();
            continue;
        }

        return op_status;
    }
}

Reconfig::Reconfig(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts, uint32_t metadata_server_timeout,
        uint32_t timeout_per_request, vector<DC*>& datacenters) : Client(id, local_datacenter_id, retry_attempts, metadata_server_timeout,
                                                                         timeout_per_request, datacenters){
    this->current_class = MIX_PROTOCOL_NAME;
}

Reconfig::~Reconfig(){
}

int Reconfig::reconfig_one_key(const string& key, const Group& old_config, uint32_t old_conf_id, const Group& new_config, uint32_t new_conf_id){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

    EASY_LOG_INIT_M(string("from ") + to_string(old_conf_id) + " to " + to_string(new_conf_id) + " for key " + key);

    unique_ptr<Timestamp> ret_ts;
    string ret_v;
    
    assert(Reconfig::send_reconfig_query(old_config, old_conf_id, new_config, new_conf_id, key, ret_ts, ret_v) == 0);
    EASY_LOG_M("send_reconfig_query done");

    EASY_LOG_M("ret_ts: " + ret_ts->get_string() + ", ret_v: " + TRUNC_STR(ret_v));
    assert(Reconfig::send_reconfig_commit(new_config, new_conf_id, key, ret_ts, ret_v) == 0);
    EASY_LOG_M("send_reconfig_commit done");

    std::string new_conf_id_str = to_string(new_conf_id);
    std::string old_conf_id_str = to_string(old_conf_id);
    assert(update_metadata_info(key, new_conf_id_str, new_config.placement, old_conf_id_str, old_config.placement) == 0);
    EASY_LOG_M("update_metadata_info done");

    assert(Reconfig::send_finish_reconfig(old_config, old_conf_id, new_config, new_conf_id, key, ret_ts) == 0);
    EASY_LOG_M("send_reconfig_finish done");

    Placement dummy_placement = Placement();
    assert(update_metadata_info(key, new_conf_id_str, new_config.placement, "", dummy_placement) == 0);
    EASY_LOG_M("update_metadata_info finished");

    return S_OK;
}

int Reconfig::reconfig(const Group& old_config, uint32_t old_conf_id, const Group& new_config, uint32_t new_conf_id){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

    vector<future<int>> rets;
    for(auto it = old_config.keys.begin(); it != old_config.keys.end(); it++){
        rets.emplace_back(async(launch::async, &Reconfig::reconfig_one_key, this, *it, old_config, old_conf_id, new_config, new_conf_id));
    }

    for(auto it = rets.begin(); it != rets.end(); it++){
        if(it->get() != S_OK){
            assert(false);
        }
    }

    return S_OK;
}

int Reconfig::update_one_metadata_server(const std::string& metadata_server_ip, uint32_t metadata_server_port, const std::string& key,
                                         const std::string& ready_confid_id, const Placement& ready_p, 
                                         const std::string& toret_confid_id, const Placement& toret_p){
    Connect c(metadata_server_ip, metadata_server_port);
    if(!c.is_connected()){
        DPRINTF(DEBUG_RECONFIG_CONTROL, "Warn: cannot connect to metadata server\n");
        return -2;
    }

    DataTransfer::sendMsg(*c, DataTransfer::serializeMDS("update", "", key, ready_confid_id, ready_p, toret_confid_id, toret_p));

    string recvd;
    if(DataTransfer::recvMsg(*c, recvd) == 1){
        string status;
        string msg;
        std::string key;
        std::string ready_id;
        std::string toret_id;
        Placement toret_placement;
        DataTransfer::deserializeMDS(recvd, status, msg, key, ready_id, toret_id, toret_placement);

        if(status != "OK"){
            DPRINTF(DEBUG_RECONFIG_CONTROL, "%s\n", msg.c_str());
            assert(false);
        }
        
        DPRINTF(DEBUG_RECONFIG_CONTROL, "metadata_server updated %s\n", msg.c_str());
    }
    else{
        DPRINTF(DEBUG_RECONFIG_CONTROL, "Error in receiving msg from Metadata Server\n");
        return -1;
    }
    return S_OK;
}

int Reconfig::update_metadata_info(const string& key, const string& ready_confid_id, const Placement& ready_p,
                                   const string& toret_confid_id, const Placement& toret_p){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    vector<future<int>> rets;
    for(uint k = 0; k < datacenters.size(); k++){
        rets.emplace_back(async(launch::async, &Reconfig::update_one_metadata_server, this, datacenters[k]->metadata_server_ip,
                                datacenters[k]->metadata_server_port, key, ready_confid_id, ready_p, toret_confid_id, toret_p));
    }

    for(auto it = rets.begin(); it != rets.end(); it++){
        if(it->get() != S_OK){
            assert(false);
        }
    }

    return S_OK;
}

int Reconfig::send_reconfig_query(const Group& old_config, uint32_t old_conf_id, 
                                  const Group& new_config, uint32_t new_conf_id, 
                                  const string& key, unique_ptr<Timestamp>& ret_ts, string& ret_v){
    
    DPRINTF(DEBUG_RECONFIG_CONTROL, "send_reconfig_query started on key \"%s\"\n", key.c_str());
    EASY_LOG_INIT_M(string("on key ") + key);

    ret_ts = nullptr;
    int op_status = 0;
    vector<strVec> ret;

    if(old_config.placement.protocol == ABD_PROTOCOL_NAME){
        vector <Timestamp> tss;
        vector <string> vs;
        uint32_t idx = -1;

        DPRINTF(DEBUG_ABD_Client, "calling failure_support_optimized.\n");
        EASY_LOG_M("calling failure_support_optimized.");

        op_status = ABD_helper_recon::failure_support_optimized("reconfig_query", key, "", "", this->retry_attempts,
                                                                old_config.placement.servers.size() - old_config.placement.quorums[this->local_datacenter_id].Q2.size() + 1, 
                                                                old_config.placement.servers,
                                                                old_config.placement.m, 
                                                                this->datacenters, 
                                                                old_config.placement.protocol, 
                                                                old_conf_id, 
                                                                new_conf_id, 
                                                                new_config.placement,
                                                                this->timeout_per_request, 
                                                                ret);

        DPRINTF(DEBUG_ABD_Client, "op_status: %d.\n", op_status);
        EASY_LOG_M("op_status is " + to_string(op_status));
        if(op_status != 0) {
            return op_status;
        }

        for(auto it = ret.begin(); it != ret.end(); it++) {
            if((*it)[0] == "OK"){
                tss.emplace_back((*it)[1]);
                vs.emplace_back((*it)[2]);
            }
            else{
                EASY_LOG_M("Bad Message");
                op_status = -1;
                break;
            }
        }

        if(op_status == 0){
            idx = Timestamp::max_timestamp3(tss);
            ret_ts.reset(new Timestamp(tss[idx]));
            ret_v = vs[idx];
            EASY_LOG_M("timestamp is " + ret_ts->get_string() + " value is " + TRUNC_STR(ret_v));
        }
        else{
            DPRINTF(DEBUG_ABD_Client, "Operation Failed.\n");
            EASY_LOG_M("Operation Failed");
        }
    }
    else{
        assert(false); // Unknown protocol
    }

    return op_status;
}

int Reconfig::send_reconfig_finalize(const Group& old_config, uint32_t old_conf_id, const string& key, unique_ptr<Timestamp>& ts,
        string& ret_v){
    
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\"\n", key.c_str());
    EASY_LOG_INIT_M(string("on key ") + key);

    bool uninitialized_key = false;
    int op_status = 0;
    vector<strVec> ret;
    vector<string> chunks;

    EASY_LOG_M("calling failure_support_optimized.");
    op_status = CAS_helper_recon::failure_support_optimized("reconfig_finalize", key, ts->get_string(), vector<string>(old_config.placement.m, ""), this->retry_attempts,
                                                      old_config.placement.quorums[this->local_datacenter_id].Q4.size(), old_config.placement.servers, old_config.placement.m,
                                                      this->datacenters, old_config.placement.protocol, old_conf_id,
                                                      this->timeout_per_request, ret);

    DPRINTF(DEBUG_CAS_Client, "op_status: %d.\n", op_status);
    EASY_LOG_M("op_status is " + to_string(op_status));
    if(op_status != 0) {
        return op_status;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){
            if((*it)[1] == "Ack"){

            }
            else if((*it)[1] == "init"){
                uninitialized_key = true;
            }
            else{
                chunks.push_back((*it)[1]);
            }
        }
        else{
            DPRINTF(DEBUG_CAS_Client, "wrong message received: %s : %s\n", (*it)[0].c_str(), (*it)[1].c_str());
            EASY_LOG_M("Bad Message");
            op_status = -1;
            break;
        }
    }

    if(op_status != 0) {
        EASY_LOG_M("operation failed");
        return op_status;
    }

    if(!uninitialized_key){
        if(chunks.size() < old_config.placement.k){
            op_status = -9;
            DPRINTF(DEBUG_CAS_Client, "chunks.size() < p.k key : %s\n", key.c_str());
            EASY_LOG_M("chunks.size() < p.k");
            return op_status;
        }

        //    char bbuf[1024*128];
        //    int bbuf_i = 0;
        //    bbuf_i += sprintf(bbuf + bbuf_i, "%s-get function value is %s\n", key.c_str(), value.c_str());
        //    for(uint t = 0; t < chunks.size(); t++){
        //        bbuf_i += sprintf(bbuf + bbuf_i, "%s-chunk[%d] = ", key.c_str(), t);
        //        for(uint tt = 0; tt < chunks[t]->size(); tt++){
        //            bbuf_i += sprintf(bbuf + bbuf_i, "%02X", chunks[t]->at(tt) & 0xff);
        ////                printf("%02X", chunks[t]->at(tt));
        //        }
        //        bbuf_i += sprintf(bbuf + bbuf_i, "\n");
        //    }
        //    printf("%s", bbuf);
        assert(liberasure.decode(ret_v, chunks, old_config.placement.m, old_config.placement.k) == 0);
        EASY_LOG_M("value is " + ret_v);
    }
    else{
        ret_v = "__Uninitiliazed";
        EASY_LOG_M("value is " + ret_v);
    }

    return op_status;
}

int Reconfig::send_reconfig_commit(const Group& new_config, uint32_t new_conf_id, const string& key, unique_ptr<Timestamp>& ts,
        const string& value){
    
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\"\n", key.c_str());
    EASY_LOG_INIT_M(string("on key ") + key);

    int op_status = 0;
    vector<strVec> ret;

    if(new_config.placement.protocol == ABD_PROTOCOL_NAME){
        DPRINTF(DEBUG_ABD_Client, "calling failure_support_optimized.\n");
        EASY_LOG_M("calling failure_support_optimized.");
        op_status = ABD_helper_recon::failure_support_optimized("reconfig_commit", key, ts->get_string(), value, this->retry_attempts,
                                                                new_config.placement.quorums[this->local_datacenter_id].Q2.size(), new_config.placement.servers,
                                                                new_config.placement.m, this->datacenters, new_config.placement.protocol, new_conf_id, 
                                                                new_conf_id, new_config.placement, this->timeout_per_request, ret);

        DPRINTF(DEBUG_ABD_Client, "op_status: %d.\n", op_status);
        EASY_LOG_M("op_status is " + to_string(op_status));
        if(op_status != 0) {
            return op_status;
        }

        for(auto it = ret.begin(); it != ret.end(); it++) {
            if((*it)[0] == "OK"){
//                DPRINTF(DEBUG_ABD_Client, "OK received for key : %s\n", key.c_str());
            }
            else{
                DPRINTF(DEBUG_ABD_Client, "Bad message received from server for key : %s\n", key.c_str());
                EASY_LOG_M("Bad Message");
                return -3; // Bad message received from server
            }
        }
    }
    else{
        assert(false); // Unknown protocol
    }

    return op_status;
}

int Reconfig::send_finish_reconfig(const Group& old_config, uint32_t old_conf_id, const Group& new_config, uint32_t new_conf_id, const string& key,
                                   unique_ptr<Timestamp>& ts){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\"\n", key.c_str());
    EASY_LOG_INIT_M(string("on key ") + key);

    int op_status = 0;
    vector<strVec> ret;

    if(old_config.placement.protocol == ABD_PROTOCOL_NAME){
        DPRINTF(DEBUG_ABD_Client, "calling failure_support_optimized.\n");
        EASY_LOG_M("calling failure_support_optimized.");
        op_status = ABD_helper_recon::failure_support_optimized("finish_reconfig", key, "", "", this->retry_attempts,
                                                                old_config.placement.servers.size(), old_config.placement.servers, old_config.placement.m,
                                                                this->datacenters, old_config.placement.protocol, old_conf_id, new_conf_id, new_config.placement,
                                                                this->timeout_per_request, ret);

        DPRINTF(DEBUG_ABD_Client, "op_status: %d.\n", op_status);
        EASY_LOG_M("op_status is " + to_string(op_status));
        if(op_status != 0) {
            return op_status;
        }

        for(auto it = ret.begin(); it != ret.end(); it++) {
            if((*it)[0] == "OK"){
//                DPRINTF(DEBUG_ABD_Client, "OK received for key : %s\n", key.c_str());
            }
            else{
                DPRINTF(DEBUG_ABD_Client, "Bad message received from server for key : %s\n", key.c_str());
                EASY_LOG_M("Bad Message");
                return -3; // Bad message received from server
            }
        }
    }
    else{
        assert(false); // Unknown protocol
    }

    return op_status;
}

int Reconfig::put(const std::string& key, const std::string& value){
    DPRINTF(DEBUG_CAS_Client, "This method should not be called.");
    return 0;
}
int Reconfig::get(const std::string& key, std::string& value){
    DPRINTF(DEBUG_CAS_Client, "This method should not be called.");
    return 0;
}
