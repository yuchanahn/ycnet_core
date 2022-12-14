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

#include "yc_framework/ycutil.hpp"

#pragma comment(lib, "ws2_32.lib")

#define ERR_TEXT(msg) std::format("{} Error: {}", #msg , ::GetLastError()).c_str()


namespace ycnet
{
    namespace core
    {
        inline RIO_EXTENSION_FUNCTION_TABLE g_rio_ft;
        inline SOCKET g_socket;

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
            std::function<void()> kill;
        };

        EXTENDED_RIO_BUF* addr_buf;

        RIO_BUFFERID send_buf_id;
        RIO_BUFFERID recv_buf_id;
        RIO_BUFFERID addr_buf_id;

        char* send_buf_ptr;
        char* recv_buf_ptr;
        char* addr_buf_ptr;

        struct endpoint_t {
            // 192.128.0.1
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
            while (send_bufs[thread_number]  | std::views::filter(is_not_used)
                                             | std::views::empty) 
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            
            for(auto& context : send_bufs[thread_number] | std::views::filter(is_not_used)) {
                char* send_offset = send_buf_ptr + context.buf.Offset;
                std::copy_n(buf, size, send_offset);
                const auto addr_in = addr.to_sockaddr_in();
                std::copy_n((char*)&addr_in, sizeof(sockaddr_in), addr_buf_ptr + context.addr_buf.Offset);

                if (!g_rio_ft.RIOSendEx(
                    g_rq,
                    &context.buf,
                    1,
                    nullptr,
                    &context.addr_buf,
                    nullptr,
                    nullptr,
                    0,
                    &context.buf))
                        return ERR_TEXT(RIOSend);
                return yc::err_opt_t(size);
            }
        }

        inline std::vector<std::vector<std::vector<pooled_recv_data_t>>> recv_data_pool;

        inline yc::err_opt_t<DWORD> bind_receive_udp(EXTENDED_RIO_BUF* buf) {
            if (const auto data = reinterpret_cast<EXTENDED_RIO_BUF*>(recv_buf_ptr + buf->Offset);
                !g_rio_ft.RIOReceiveEx(
                    g_rq,
                    data,
                    1,
                    nullptr,
                    &addr_buf[buf->index],
                    nullptr,
                    nullptr,
                    0,
                    data)) return ERR_TEXT(RIOReceive);
            return yc::err_opt_t(buf->index);
        }
        
        inline yc::err_opt_t<rio_udp_server_controller> UDP_CORE(
            const int thread_count,
            const u_short port,
            const std::function<void(const char*, int, endpoint_t)>& recv_callback
        ) {
            worker_threads.reserve(thread_count);
            
            for (int i = 0; i < thread_count; ++i) {
                recv_data_pool.emplace_back();
                for (int j = 0; j < thread_count; ++j) {
                    if (i == j) continue;
                    recv_data_pool[i].emplace_back();
                    for (int k = 0; k < RIO_PENDING_RECVS; ++k) { recv_data_pool[i][j].emplace_back(); }
                }
            }

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
                              &function_table_id,
                              sizeof(GUID),
                              &g_rio_ft,
                              sizeof g_rio_ft,
                              &dw_bytes,
                              nullptr,
                              nullptr))
                return ERR_TEXT(WSAIoctl);



            // TODO :
            // CQ, RQ 가 Thread Safe 하지 않는다?
            // CQ, RQ를 Thread당 하나씩 만들어 둔다.
            // 방법을 찾아보자!

            
            
            g_cq = g_rio_ft.RIOCreateCompletionQueue(RIO_PENDING_RECVS + RIO_PENDING_SENDS, nullptr);
            if (g_cq == RIO_INVALID_CQ) return ERR_TEXT(RIOCreateCompletionQueue);

            /// RIO RQ 생성
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

            /// SEND용 RIO 버퍼 등록
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
                buffer_ptr->thread_number = min(j / (RIO_PENDING_RECVS / thread_count), (DWORD)thread_count - 1);
                buffer_ptr->index = j;
                buffer_ptr->BufferId = recv_buf_id;
                buffer_ptr->Offset = offset;
                buffer_ptr->Length = RECV_BUFFER_SIZE;
                offset += RECV_BUFFER_SIZE;

                if (auto r = bind_receive_udp(buffer_ptr); !r.has_value()) return r.err.c_str();
            }

            for (int i = 0; i < thread_count; ++i) {
                auto thread_number = i;
                
                worker_threads.emplace_back([thread_number, recv_callback, thread_count] {
                    while (true) {
                        RIORESULT results[RIO_MAX_RESULTS] = {};

                        const ULONG num_results = g_rio_ft.RIODequeueCompletion(g_cq, results, RIO_MAX_RESULTS);

                        
                        
                        if (0 == num_results) {
                            bool do_something = false;
                            for(int i = 0; i < thread_count; ++i) {
                                if (i == thread_number) continue;
                                for (auto& data : recv_data_pool[i][thread_number] | std::views::filter(is_used)) {
                                    recv_callback(recv_buf_ptr + data.read_buf_offset, static_cast<int>(data.read_buf_size), data.endpoint);
                                    data.is_used = false;
                                    if(auto r = bind_receive_udp(reinterpret_cast<EXTENDED_RIO_BUF*>(recv_buf_ptr + data.read_buf_offset));
                                        !r.has_value()) return r.err.c_str();
                                    do_something = true;
                                }
                            }
                            if(!do_something) std::this_thread::sleep_for(std::chrono::milliseconds(1));
                            continue;
                        }
                        for (DWORD i = 0; i < num_results; ++i) {
                            if (const auto buffer_ptr = reinterpret_cast<EXTENDED_RIO_BUF*>(results[i].RequestContext);
                                OP_RECV == buffer_ptr->operation) {
                                const char* source = recv_buf_ptr + buffer_ptr->Offset;

                                const auto addr_ptr = reinterpret_cast<sockaddr_in*>(addr_buf + buffer_ptr->Offset);
                                endpoint_t endpoint{};
                                endpoint.set_ip(addr_ptr);

                                if (buffer_ptr->thread_number == thread_number) {
                                    recv_callback(source, static_cast<int>(results[i].BytesTransferred), endpoint);

                                    if(auto r = bind_receive_udp(buffer_ptr); !r.has_value()) return r.err.c_str();
                                } else {
                                    auto& recv_target = recv_data_pool[thread_number][buffer_ptr->thread_number];

                                    // busy wait
                                    while (recv_target  | std::views::filter(is_not_used)
                                                        | std::views::empty) {
                                        std::this_thread::sleep_for(std::chrono::microseconds(1));
                                    }
                                    for (auto& item : recv_target | std::views::filter(is_not_used)
                                                                  | std::views::take(1)) item = {
                                        buffer_ptr->Offset,
                                        results[i].BytesTransferred,
                                        endpoint,
                                        true
                                    };
                                }
                            }
                            else if (OP_SEND == buffer_ptr->operation) {}
                            else break;
                        }
                    }
                    std::stringstream ss;
                    ss << std::this_thread::get_id();
                    std::string str = ss.str();

                    return std::format(" -- thread[{} : id-{}] exit -- ", thread_number, str).c_str();
                });
            }

            return yc::err_opt_t(rio_udp_server_controller{
                [] {
                    //if (0 == ::PostQueuedCompletionStatus(g_h_iocp, 0, CK_STOP, nullptr))
                        //return ERR_TEXT(PostQueuedCompletionStatus);

                    std::ranges::for_each(worker_threads, [](std::thread& t) { t.join(); });

                    if (SOCKET_ERROR == ::closesocket(g_socket)) return ERR_TEXT(closesocket);

                    g_rio_ft.RIOCloseCompletionQueue(g_cq);
                    g_rio_ft.RIODeregisterBuffer(send_buf_id);
                    g_rio_ft.RIODeregisterBuffer(recv_buf_id);
                    g_rio_ft.RIODeregisterBuffer(addr_buf_id);

                    return "SUCCESS!";
                }
            });
        }
    }
}
