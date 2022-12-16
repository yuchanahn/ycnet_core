#pragma once
#include <algorithm>
#include <SDKDDKVer.h>
#include <tchar.h>
#include <WinSock2.h>
#include <MSWsock.h>
#include <iostream>
#include <format>
#include <functional>
#include <sstream>
#include <vector>
#include <thread>
#include <WS2tcpip.h>
#include <ranges>
#include <unordered_map>

#include "yc_framework/ycutil.hpp"
#include "yc_framework/srw_lock.hpp"

#pragma comment(lib, "ws2_32.lib")

#define ERR_TEXT(msg) std::format("{} Error: {}", #msg , ::GetLastError()).c_str()

namespace ycnet
{
    namespace core
    {
        inline RIO_EXTENSION_FUNCTION_TABLE g_rio_ft;
        inline SOCKET g_socket;
        inline srw_lock g_cqrq_lock;
        
        enum RIO_Config {
            RIO_PENDING_RECVS = 1000,
            RIO_PENDING_SENDS = 1000,
            RECV_BUFFER_SIZE = 1024,
            SEND_BUFFER_SIZE = 1024,

            ADDR_BUFFER_SIZE = 64,

            ADDR_BUFFER_COUNT = (RIO_PENDING_RECVS + RIO_PENDING_SENDS) * ADDR_BUFFER_SIZE,

            RIO_MAX_RESULTS = 1000
        };

        inline std::vector<std::thread> worker_threads;

        enum COMPLETION_KEY {
            CK_STOP = 0,
            CK_START = 1
        };

        enum OPERATION_TYPE {
            OP_NONE = 0,
            OP_RECV = 1,
            OP_SEND = 2
        };

        struct EXTENDED_RIO_BUF : RIO_BUF {
            OPERATION_TYPE operation;
            int thread_number;
            DWORD index;
        };

        inline char* allocate_buf(const DWORD size) {
            return static_cast<char*>(
                VirtualAllocEx(
                    GetCurrentProcess(),
                    nullptr,
                    size,
                    MEM_COMMIT | MEM_RESERVE,
                    PAGE_READWRITE));
        }

        struct rio_udp_server_controller {
            std::function<yc::err_opt_t<int>()> kill;
        };

        EXTENDED_RIO_BUF* addr_buf;

        RIO_BUFFERID send_buf_id;
        RIO_BUFFERID recv_buf_id;
        RIO_BUFFERID addr_buf_id;

        char* send_buf_ptr;
        char* recv_buf_ptr;
        char* addr_buf_ptr;

        struct endpoint_t {
            u_char ip1;
            u_char ip2;
            u_char ip3;
            u_char ip4;
            u_short port;

            std::string to_string() const {
                return std::to_string(ip1) + "." + std::to_string(ip2) + "." + std::to_string(ip3) + "." +
                    std::to_string(ip4);
            }
            sockaddr_in to_sockaddr_in() const {
                sockaddr_in addr;
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr.S_un.S_un_b.s_b1 = ip1;
                addr.sin_addr.S_un.S_un_b.s_b2 = ip2;
                addr.sin_addr.S_un.S_un_b.s_b3 = ip3;
                addr.sin_addr.S_un.S_un_b.s_b4 = ip4;
                return addr;
            }
            void set_ip(const sockaddr_in* addr) {
                ip1 = addr->sin_addr.S_un.S_un_b.s_b1;
                ip2 = addr->sin_addr.S_un.S_un_b.s_b2;
                ip3 = addr->sin_addr.S_un.S_un_b.s_b3;
                ip4 = addr->sin_addr.S_un.S_un_b.s_b4;
                port = ntohs(addr->sin_port);
            }

            endpoint_t(u_char ip1, u_char ip2, u_char ip3, u_char ip4, u_short port) : ip1(ip1), ip2(ip2), ip3(ip3), ip4(ip4), port(port) {}
            endpoint_t() : ip1(0), ip2(0), ip3(0), ip4(0), port(0) {}
            friend std::hash<endpoint_t>;
            friend bool operator==(const endpoint_t& my, const endpoint_t& other) {
                return my.ip1 == other.ip1 && my.ip2 == other.ip2 && my.ip3 == other.ip3 && my.ip4 == other.ip4 && my.port == other.port;
            }
        };


        
        struct pooled_recv_data_t {
            DWORD read_buf_size;
            DWORD read_buf_offset;
            endpoint_t endpoint;
            volatile bool is_used;
        };

        inline RIO_CQ g_cq;
        inline RIO_RQ g_rq;
        
        struct send_buf_t {
            EXTENDED_RIO_BUF buf;
            EXTENDED_RIO_BUF addr_buf;
            volatile bool is_used;
        };

        
        inline std::vector<std::vector<send_buf_t>> send_bufs;
        inline bool is_running = true;
        static auto is_not_used = [](const auto& x) { return !x.is_used; };
        static auto is_used = [](const auto& x) { return x.is_used; };
        
        /** 
         * \brief send가 thread safe 하지 않기 때문에 호출 할 때 알아서 해줘야함.
         * \param buf 전송할 데이터 
         * \param size 전송할 데이터 크기
         * \param addr 전송할 주소
         * \param thread_number 전송할 스레드 번호, 스레드 번호는 스레드의 개수보다 작아야함.
         * \return 전송한 바이트 크기
         */
        yc::err_opt_t<int> send_udp(const char* buf, int size, const endpoint_t addr, const int thread_number) {
            // busy wait
            while (std::ranges::empty(send_bufs[thread_number] | std::views::filter(is_not_used))) 
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            
            for(auto& context : send_bufs[thread_number] | std::views::filter(is_not_used)) {
                char* send_offset = send_buf_ptr + context.buf.Offset;
                std::copy_n(buf, size, send_offset);
                const auto addr_in = addr.to_sockaddr_in();
                std::copy_n((char*)&addr_in, sizeof(sockaddr_in), addr_buf_ptr + context.addr_buf.Offset);
                g_cqrq_lock.w_lock();
                const auto r = g_rio_ft.RIOSendEx(
                    g_rq,
                    &context.buf,
                    1,
                    nullptr,
                    &context.addr_buf,
                    nullptr,
                    nullptr,
                    0,
                    &context.buf);
                g_cqrq_lock.w_unlock();
                
                if (!r)
                        return ERR_TEXT(RIOSend);
                return yc::err_opt_t(size);
            }
        }

        inline yc::err_opt_t<DWORD> bind_receive_udp(EXTENDED_RIO_BUF* buf) {
            g_cqrq_lock.w_lock();
            const auto r = g_rio_ft.RIOReceiveEx(
                    g_rq,
                    buf,
                    1,
                    nullptr,
                    &addr_buf[buf->index],
                    nullptr,
                    nullptr,
                    0,
                    buf);
            g_cqrq_lock.w_unlock();
            if (!r) return ERR_TEXT(RIOReceive);
            return yc::err_opt_t(buf->index);
        }
        
        inline yc::err_opt_t<rio_udp_server_controller> UDP_CORE(
            const int thread_count,
            const u_short port,
            const std::function<void(const char*, int, endpoint_t)>& recv_callback,
            const std::function<void(const char*)>& io_thread_msg_callback
        ) {
            worker_threads.reserve(thread_count);

            static WSADATA data;
            if (::WSAStartup(0x202, &data)) return ERR_TEXT(WSAStartup);

            g_socket = WSASocket(AF_INET, SOCK_DGRAM, IPPROTO_UDP, nullptr, 0, WSA_FLAG_REGISTERED_IO);
            if (g_socket == INVALID_SOCKET) return ERR_TEXT(WSASocket);

            static sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            addr.sin_addr.s_addr = INADDR_ANY;

            if (SOCKET_ERROR == ::bind(g_socket, reinterpret_cast<sockaddr*>(&addr), sizeof addr)) return
                ERR_TEXT(Bind);

            static GUID function_table_id = WSAID_MULTIPLE_RIO;
            static DWORD dw_bytes = 0;

            if (0 != WSAIoctl(g_socket,
                              SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
                              &function_table_id, sizeof GUID,
                              &g_rio_ft, sizeof g_rio_ft,
                              &dw_bytes,
                              nullptr,
                              nullptr))
                return ERR_TEXT(WSAIoctl);

            g_cq = g_rio_ft.RIOCreateCompletionQueue(RIO_PENDING_RECVS + RIO_PENDING_SENDS, nullptr);
            if (g_cq == RIO_INVALID_CQ) return ERR_TEXT(RIOCreateCompletionQueue);

            g_rq = g_rio_ft.RIOCreateRequestQueue(
                g_socket,
                RIO_PENDING_RECVS,
                1,
                RIO_PENDING_SENDS,
                1,
                g_cq,
                g_cq,
                nullptr);
            
            if (g_rq == RIO_INVALID_RQ) return ERR_TEXT(RIOCreateRequestQueue);
            {
                send_bufs.reserve(thread_count);
                send_buf_ptr = allocate_buf(RIO_PENDING_SENDS * SEND_BUFFER_SIZE);
                send_buf_id = g_rio_ft.RIORegisterBuffer(send_buf_ptr, RIO_PENDING_SENDS * SEND_BUFFER_SIZE);
                if (send_buf_id == RIO_INVALID_BUFFERID) return ERR_TEXT(RIORegisterBuffer);
                DWORD offset = 0;
                const auto size = RIO_PENDING_SENDS / thread_count;
                
                for(int i = 0; i < thread_count; ++i) {
                    send_bufs.emplace_back();
                    send_bufs[i].reserve(size);
                    for (int j = 0; j < size; ++j) {
                        send_bufs[i].emplace_back();
                        auto& [buf, addr_buf_, is_used] = send_bufs[i].back();
                        buf.operation = OP_SEND;
                        buf.BufferId = send_buf_id;
                        buf.Offset = offset;
                        buf.Length = SEND_BUFFER_SIZE;

                        offset += SEND_BUFFER_SIZE;
                    }
                }
            }

            addr_buf_ptr = allocate_buf(ADDR_BUFFER_COUNT);
            addr_buf_id = g_rio_ft.RIORegisterBuffer(addr_buf_ptr, ADDR_BUFFER_COUNT);

            if (addr_buf_id == RIO_INVALID_BUFFERID) return ERR_TEXT(RIORegisterBuffer);

            DWORD offset = 0;
            addr_buf = new EXTENDED_RIO_BUF[RIO_PENDING_RECVS];

            for (DWORD j = 0; j < RIO_PENDING_RECVS; ++j) {
                EXTENDED_RIO_BUF* p_buffer = addr_buf + j;

                p_buffer->operation = OP_NONE;
                p_buffer->BufferId = addr_buf_id;
                p_buffer->Offset = offset;
                p_buffer->Length = ADDR_BUFFER_SIZE;

                offset += ADDR_BUFFER_SIZE;
            }

            const auto size = RIO_PENDING_SENDS / thread_count;
            for(int i = 0; i < thread_count; ++i) {
                for (int j = 0; j < size; ++j) {
                    auto& [buf, addr_buf_, is_used] = send_bufs[i][j];

                    addr_buf_.operation = OP_NONE;
                    addr_buf_.BufferId = addr_buf_id;
                    addr_buf_.Offset = offset;
                    addr_buf_.Length = ADDR_BUFFER_SIZE;
                    offset += ADDR_BUFFER_SIZE;
                }
            }
            
            recv_buf_ptr = allocate_buf(RIO_PENDING_RECVS * RECV_BUFFER_SIZE);
            recv_buf_id = g_rio_ft.RIORegisterBuffer(recv_buf_ptr, RIO_PENDING_RECVS * RECV_BUFFER_SIZE);
            if (recv_buf_id == RIO_INVALID_BUFFERID) return ERR_TEXT(RIORegisterBuffer);
            offset = 0;
            const auto buf = new EXTENDED_RIO_BUF[RIO_PENDING_RECVS];

            for (DWORD j = 0; j < RIO_PENDING_RECVS; ++j) {
                EXTENDED_RIO_BUF* buffer_ptr = buf + j;

                buffer_ptr->operation = OP_RECV;
                buffer_ptr->index = j;
                buffer_ptr->BufferId = recv_buf_id;
                buffer_ptr->Offset = offset;
                buffer_ptr->Length = RECV_BUFFER_SIZE;
                offset += RECV_BUFFER_SIZE;

                if (auto r = bind_receive_udp(buffer_ptr); !r.has_value()) return r.err.c_str();
            }

            for (int i = 0; i < thread_count; ++i) {
                worker_threads.emplace_back([thread_number = i, recv_callback, io_thread_msg_callback] {
                    while (is_running) {
                        RIORESULT results[RIO_MAX_RESULTS] = {};

                        g_cqrq_lock.w_lock();
                        const ULONG r_num = g_rio_ft.RIODequeueCompletion(g_cq, results, RIO_MAX_RESULTS);
                        g_cqrq_lock.w_unlock();
                        
                        if (0 == r_num) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                            continue;
                        }
                        for (DWORD i = 0; i < r_num; ++i) {
                            if (const auto buffer_ptr = reinterpret_cast<EXTENDED_RIO_BUF*>(results[i].RequestContext);
                                OP_RECV == buffer_ptr->operation) {
                                const char* source = recv_buf_ptr + buffer_ptr->Offset;
                                const auto addr_ptr = reinterpret_cast<sockaddr_in*>(addr_buf_ptr + addr_buf[buffer_ptr->index].Offset);
                                endpoint_t endpoint{};
                                endpoint.set_ip(addr_ptr);
                                recv_callback(source, static_cast<int>(results[i].BytesTransferred), endpoint);
                                if(auto r = bind_receive_udp(buffer_ptr); !r.has_value()) io_thread_msg_callback(r.err.c_str());
                            }
                            else if (OP_SEND == buffer_ptr->operation) {
                                send_bufs[buffer_ptr->thread_number][buffer_ptr->index].is_used = false;
                            }
                            else {
                                io_thread_msg_callback(ERR_TEXT(Unknown operation));
                                break;
                            }
                        }
                    }
                    std::stringstream ss;
                    ss << std::this_thread::get_id();
                    std::string str = ss.str();
                    io_thread_msg_callback(std::format(" -- thread[{} : id-{}] exit -- \n", thread_number, str).c_str());
                });
            }

            return yc::err_opt_t(rio_udp_server_controller{
                []()->yc::err_opt_t<int> {
                    is_running = false;
                    std::ranges::for_each(worker_threads, [](std::thread& t) { t.join(); });

                    if (SOCKET_ERROR == ::closesocket(g_socket)) return ERR_TEXT(closesocket);

                    g_rio_ft.RIOCloseCompletionQueue(g_cq);
                    g_rio_ft.RIODeregisterBuffer(send_buf_id);
                    g_rio_ft.RIODeregisterBuffer(recv_buf_id);
                    g_rio_ft.RIODeregisterBuffer(addr_buf_id);

                    return yc::err_opt_t(1);
                }
            });
        }
    }
}

template <class T>
void hash_combine(std::size_t & s, const T & v)
{
    std::hash<T> h;
    s^= h(v) + 0x9e3779b9 + (s<< 6) + (s>> 2);
}

template <>
struct std::hash<ycnet::core::endpoint_t> {
    size_t operator()(const ycnet::core::endpoint_t &ep) const noexcept { 
        size_t hash = 0;
        hash_combine(hash, ep.ip1);
        hash_combine(hash, ep.ip2);
        hash_combine(hash, ep.ip3);
        hash_combine(hash, ep.ip4);
        hash_combine(hash, ep.port);
        return hash;
    }                                                                    
};