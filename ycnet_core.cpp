#include "ycnet_core.hpp"

struct client_t {
    ycnet::core::endpoint_t endpoint{};
    std::string name;
    int my_thread_number{};
};

int main()
{
    std::unordered_map<ycnet::core::endpoint_t, client_t> clients;
    srw_lock clients_lock;
    
    const auto server = ycnet::core::UDP_CORE(4, 1234, [&clients, &clients_lock]
        (const char* buf, const int len, const ycnet::core::endpoint_t ep, int thread_num) {
        
        if(!clients.contains(ep)) {
            w_srw_lock_guard g{ clients_lock };
            clients[ep] = client_t{ ep, std::string(buf, len), 0 };
            std::cout << "New client: " << clients[ep].name << std::endl;
        }else {
            std::cout << "recv: " << buf << std::endl;
            std::cout << "from: " << ep.to_string() << std::endl;
            std::vector<ycnet::core::endpoint_t> r;
            r.reserve(clients.size());
            {
                w_srw_lock_guard g{ clients_lock };
                for(const auto& key : clients | std::views::keys) {
                    r.emplace_back(key);
                }
            }
            for(const auto& _ep : r) {
                send_udp(buf, len, _ep, thread_num);
            }
        }
    }, printf);

    if(!server.has_value()) {
        printf("server error : %s\n", server.err.c_str());
        return 0;
    }

    //std::this_thread::sleep_for(std::chrono::seconds(3));
    //const auto kill = server->kill();
    //printf("%s\n", kill.has_value() ? "server killed" : kill.err.c_str());
    
    while(true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}
