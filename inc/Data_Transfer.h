#ifndef _DATA_TRANSFER_H_
#define _DATA_TRANSFER_H_

#include <sys/socket.h>
#include <sys/types.h>
#include <cstdlib>
#include <cstdint>
#include <cstdio>
#include <string>
#include <arpa/inet.h>
#include "gbuffer.pb.h"
#include "Util.h"
#include <cerrno>
#include <mutex>
#include <future>

typedef std::vector <std::string> strVec;

class DataTransfer{
public:
    static int sendAll(int sock, const void* data, int data_size);
    
    static int sendMsg(int sock, const std::string& out_str);
    
    static int recvAll(int sock, void* buf, int data_size);
    
    static int recvMsg(int sock, std::string& data);
    
    static int recvMsg_async(const int sock, std::promise <std::string>&& data_set);
    
    static std::string serialize(const strVec& data);
    
//    static std::string serializePrp(const Properties& properties_p);
    
//    static std::string serializePlacement(const Placement& placement);
    
    static strVec deserialize(std::string& data);
    
//    static Properties* deserializePrp(std::string& data);
    
//    static Placement* deserializePlacement(const std::string& data);
    
//    static std::string serializeCFG(const Placement& pp);
    
//    static Placement deserializeCFG(std::string& data);
    
    
    static std::string serializeMDS(const std::string& status, const std::string& msg, const std::string& key,
                                    const std::string& ready_conf_id, const Placement& ready_placement, 
                                    const std::string& toret_conf_id, const Placement& toret_placement);

    static std::string serializeMDS(const std::string& status, const std::string& msg, const std::string& key,
                                    const std::string& ready_conf_id, const Placement& ready_placement);

    static std::string serializeMDS(const std::string& status, const std::string& msg);


    static Placement deserializeMDS(const std::string& data, std::string& status, std::string& msg, std::string& key,
                                    std::string& ready_conf_id, std::string& toret_conf_id, Placement& toret_placement);

};


#endif
