#include <stdlib.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <infiniband/arch.h>
#include <rdma/rdma_cma.h>
#include <string>

/*declarations*/

enum
{
    RESOLVE_TIMEOUT_MS = 5000,
};

struct pdata
{
    uint64_t buf_va;
    uint32_t buf_rkey;
};

struct pdata rep_pdata;
struct rdma_event_channel *cm_channel;
struct rdma_cm_id *listen_id;
struct rdma_cm_id *cm_id;
struct rdma_cm_event *event;
struct rdma_conn_param conn_param = {};
struct ibv_pd *pd;
struct ibv_comp_channel *comp_chan;
struct ibv_cq *cq;
struct ibv_cq *evt_cq;
struct ibv_mr *mr;
struct ibv_qp_init_attr qp_attr = {};
struct ibv_sge sge;
struct ibv_send_wr send_wr = {};
struct ibv_send_wr *bad_send_wr;
struct ibv_recv_wr recv_wr = {};
struct ibv_recv_wr *bad_recv_wr;
struct ibv_wc wc;

void *cq_context;
struct sockaddr_in sin1;
char *buf;
int err;

/* Set up RDMA CM structures */

/*returns 0 on sucess , 1 on failure*/

int set_rdma_cm_str()
{
    cm_channel = rdma_create_event_channel();

    if (!cm_channel)
        return 1;

    err = rdma_create_id(cm_channel, &listen_id, NULL, RDMA_PS_TCP);

    if (err)
        return err;

    sin1.sin_family = AF_INET;

    sin1.sin_port = htons(20069);

    sin1.sin_addr.s_addr = INADDR_ANY;

    return 0;
}

/*binding socket and listening at the port for connection request*/

/*returns 0 on sucess , 1 on failure*/

int bindsock()
{
    err = rdma_bind_addr(listen_id, (struct sockaddr *)&sin1);

    if (err)
        return err;

    err = rdma_listen(listen_id, 1);

    if (err)
        return err;

    err = rdma_get_cm_event(cm_channel, &event);

    if (err)
        return err;

    if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST)
        return 1;

    cm_id = event->id;

    rdma_ack_cm_event(event);

    return 0;
}

/* Create verbs objects now that we know which device to use */

int create_verb_obj()

{
    pd = ibv_alloc_pd(cm_id->verbs);

    if (!pd)
        return 1;

    comp_chan = ibv_create_comp_channel(cm_id->verbs);

    if (!comp_chan)
        return 1;

    cq = ibv_create_cq(cm_id->verbs, 2, NULL, comp_chan, 0);

    if (!cq)
        return 1;

    if (ibv_req_notify_cq(cq, 0))
        return 1;

    buf = (char *)calloc(10000, sizeof(char));
   
    if (!buf)
        return 1;

    mr = ibv_reg_mr(pd, buf, 10000 * sizeof(char),IBV_ACCESS_LOCAL_WRITE |

                        IBV_ACCESS_REMOTE_READ |

                        IBV_ACCESS_REMOTE_WRITE);

    if (!mr)
        return 1;

    qp_attr.cap.max_send_wr = 1;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_wr = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.qp_type = IBV_QPT_RC;

    err = rdma_create_qp(cm_id, pd, &qp_attr);

    if (err)
        return err;
    else
        return 0;
}

/*Posting receive before accepting connection */

/*returns 0 on sucess , 1 on failure*/

int post_recv()
{
    sge.addr = (uintptr_t)buf;
    sge.length = 10000 * sizeof(char);
    sge.lkey = mr->lkey;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;

    if (ibv_post_recv(cm_id->qp, &recv_wr, &bad_recv_wr))

        return 1;

    rep_pdata.buf_va = htonl((uintptr_t)buf);
    rep_pdata.buf_rkey = htonl(mr->rkey);
    conn_param.responder_resources = 1;
    conn_param.private_data = &rep_pdata;
    conn_param.private_data_len = sizeof rep_pdata;

    return 0;
}

/*accepting connection*/

/*returns 0 on sucess , 1 on failure*/

int accept_conn()
{
    err = rdma_accept(cm_id, &conn_param);

    if (err)
        return 1;

    err = rdma_get_cm_event(cm_channel, &event);

    if (err)
        return err;

    if (event->event != RDMA_CM_EVENT_ESTABLISHED)
        return 1;

    rdma_ack_cm_event(event);

    return 0;
}

/* Wait for receive completion*/

/*returns 0 on sucess , 1 on failure*/

int wait_recv_comp()
{
    if (ibv_get_cq_event(comp_chan, &evt_cq, &cq_context))
        return 1;

    if (ibv_req_notify_cq(cq, 0))
        return 1;

    if (ibv_poll_cq(cq, 1, &wc) < 1)
        return 1;

    printf("** Recevied Data : %s\n",buf);

    if (wc.status != IBV_WC_SUCCESS)
        return 1;

    return 0;
}

/*scaling operation*/

void operation()
{
   int len = strlen(buf);
   buf[len] = '!';
   printf("** Send Data : %s\n",buf);
}

/*sending result and waiting for receive completion*/

/*returns 0 on sucess , 1 on failure*/

int post_conn()
{
    sge.addr = (uintptr_t)buf;
    sge.length = 10000 * sizeof(char);
    sge.lkey = mr->lkey;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;

    if (ibv_post_send(cm_id->qp, &send_wr, &bad_send_wr))
        return 1;

    if (ibv_get_cq_event(comp_chan, &evt_cq, &cq_context))
        return 1;

    if (ibv_poll_cq(cq, 1, &wc) < 1)
        return 1;

    if (wc.status != IBV_WC_SUCCESS)
        return 1;

    ibv_ack_cq_events(cq, 2);

    return 0;
}

int main(int argc, char *argv[])
{
    int err;
    err = set_rdma_cm_str();
    
    if (!err)
    {
        err = bindsock();

        if (!err)
        {
            err = create_verb_obj();

            if (!err)
            {
                err = post_recv();

                if (!err)
                {
                    err = accept_conn();

                    if (!err)
                    {
                        err = wait_recv_comp();

                        if (!err)
                        {
                            operation();
                            err = post_conn();

                            if (err)
                            {
                                printf("Error in post connection\n");
                            }
                        }
                        else
                            printf("Error in wait recv connection\n");
                    }
                    else
                        printf("Error in accept connection\n");
                }
                else
                    printf("Error in post receive\n");
            }
            else
                printf("Error in creating verb object\n");
        }
        else
            printf("Error in creating rdma cm structures\n");
    }

    printf("\nData sent sucessfully.\n");
}
