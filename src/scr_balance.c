/*
 * Maksym Planeta, 2016
 */

#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <inttypes.h>

#include "scr_globals.h"

#define NSEC 1
#define USEC 1000*NSEC
#define MSEC 1000*USEC
#define  SEC 1000*MSEC

static int promise_fd = -1;
static char *promise_file_name = NULL;

static struct timespec last_step;
static struct timeval last_timeval;
static MPI_Datatype MPI_WORK_ITEM = 0;

static const double scr_imbalance_threshold = 1.1;

/*
 * Comparison function for qsort to sort doubles in descending order.
 */

struct work_item {
  int id;
  int node;
  double work;
};

static int compare_work_item_work(const void *a, const void *b) {
  const struct work_item *aa = a;
  const struct work_item *bb = b;
  if (aa->work > bb->work) return -1;
  if (aa->work < bb->work) return 1;
  return 0;
}

static int compare_work_item_id(const void *a, const void *b) {
  const struct work_item *aa = a;
  const struct work_item *bb = b;
  if (aa->id > bb->id) return 1;
  if (aa->id < bb->id) return -1;
  return 0;
}

static int compare_work_item_node(const void *a, const void *b) {
  const struct work_item *aa = a;
  const struct work_item *bb = b;
  if (aa->node > bb->node) return 1;
  if (aa->node < bb->node) return -1;
  return 0;
}

static int compare_work_item_ptr(const void *a, const void *b) {
  const struct work_item * const *pa = a;
  const struct work_item * const *pb = b;
  const struct work_item *aa = *pa;
  const struct work_item *bb = *pb;
  if (aa->work > bb->work) return -1;
  if (aa->work < bb->work) return 1;
  return 0;
}

static int compare_int(const void *a, const void *b)
{
  const int *aa = a;
  const int *bb = b;
  if (*aa > *bb) return 1;
  if (*bb > *aa) return 0;
  return 0;
}

static scr_reddesc_migration *scr_balance_reddesc_migration()
{
  scr_reddesc *reddesc = scr_reddesc_for_migration(scr_nreddescs, scr_reddescs);
  scr_reddesc_migration *state = (scr_reddesc_migration *)reddesc->copy_state;

  assert(state);

  return state;
}

static void scr_balance_reddesc_set_chunks(struct work_item *chunks)
{
  scr_reddesc_migration *state = scr_balance_reddesc_migration();

  assert(state->chunks == NULL);

  state->chunks = chunks;
}

void scr_balance_timestamp(const char *message)
{
  if (!scr_balancer) {
    return;
  }

  MPI_Barrier(scr_comm_world);

  if (scr_my_rank_world == 0) {
    /* Print current time in milliseconds to log the work */
    struct timespec cur_step;
    clock_gettime(CLOCK_REALTIME, &cur_step);
    long long unsigned time = cur_step.tv_sec * 1000 + cur_step.tv_nsec / 1000000;
    printf("SCR_BALANCER_TOKEN: %s: %llu\n", message, time);
  }
}

void scr_balance_timestamp_nb(const char *message)
{
  if (!scr_balancer) {
    return;
  }

  if (scr_my_rank_world == 0) {
    /* Print current time in milliseconds to log the work */
    struct timespec cur_step;
    clock_gettime(CLOCK_REALTIME, &cur_step);
    long long unsigned time = cur_step.tv_sec * 1000 + cur_step.tv_nsec / 1000000;
    printf("SCR_BALANCER_TOKEN: %s: %llu\n", message, time);
  }
}

void __scr_stat_emit(const char *message, int64_t value)
{
  int64_t max, min, avg, total;

  MPI_Reduce(&value, &max,   1, MPI_INT64_T, MPI_MAX, 0, scr_comm_world);
  MPI_Reduce(&value, &min,   1, MPI_INT64_T, MPI_MIN, 0, scr_comm_world);
  MPI_Reduce(&value, &total, 1, MPI_INT64_T, MPI_SUM, 0, scr_comm_world);

  if (scr_my_rank_world == 0) {
    printf("SCR_STAT_TOKEN: %s: "
           "max: %"PRId64", "
           "min: %"PRId64", "
           "avg: %"PRId64", "
           "tot: %"PRId64"\n",
           message, max, min, total/scr_ranks_world, total);
  }
}

#define MPI_Aint_diff(addr1, addr2) ((MPI_Aint) ((char *) (addr1) - (char *) (addr2)))

int scr_balance_init(void)
{
  scr_stat_emit(scr_stat_file_sent);
  scr_stat_emit(scr_stat_file_recv);

  /* Init value of last time step for imbalance measurement. */
  memset(&last_step, 0, sizeof(last_step));

  /* Create work item type */
  {
#define NITEMS 3
    int blocklengths[NITEMS] = {1, 1, 1};
    MPI_Datatype types[NITEMS] = {MPI_INT, MPI_INT, MPI_DOUBLE};
    MPI_Aint disp[NITEMS];

    MPI_Aint base, extent;
    struct work_item sample;
    MPI_Get_address(&sample.id, disp);
    MPI_Get_address(&sample.node, disp+1);
    MPI_Get_address(&sample.work, disp+2);
    base = disp[0];
    for (int i = 0; i < 3; i++)
      disp[i] = MPI_Aint_diff(disp[i], base);

    MPI_Type_create_struct(NITEMS, blocklengths, disp, types, &MPI_WORK_ITEM);
    MPI_Type_commit(&MPI_WORK_ITEM);
#undef NITEMS
  }

  /* Initialize pipe */
  {
    int node_id;
    int rank_node;

    MPI_Comm_rank(scr_comm_node_across, &node_id);
    MPI_Comm_rank(scr_comm_node, &rank_node);

    if (rank_node == 0) {
      char *file;
      if ((file = getenv("SCR_BALANCE_PROMISE")) != NULL) {
        /* There is a pipe which we should use to send a promise to
           migrate. */
        int fd;
        if (access(file, F_OK ) != -1) {
          // file exists
          fd = scr_open(file, O_WRONLY);
          if (fd < 0) {
            scr_err("Opening file for write: scr_open(%s) errno=%d %s @ %s:%d",
                    file, errno, strerror(errno), __FILE__, __LINE__);
            goto error;
          }

          promise_fd = fd;
          promise_file_name = file;
        } else {
          // wrong node
        }
      }
    }
  }

  return 0;

 error:
  return -1;
}

int scr_balance_finalize_promise(void)
{
  if (promise_file_name) {
    const char *message = "PROMISE_END\n";
    scr_write(promise_file_name, promise_fd, message, strlen(message));
    scr_close(promise_file_name, promise_fd);
  }

  /* we're no longer in an initialized state */
  scr_initialized = 0;

  return 0;
}

int scr_balance_finalize(void)
{
  MPI_Type_free(&MPI_WORK_ITEM);

  return 0;
}

static int diff_time(struct timespec *x, struct timespec *y, struct timespec *result)
{
    /* Perform the carry for the later subtraction by updating y. */
    if (x->tv_nsec < y->tv_nsec) {
      long nsec = (y->tv_nsec - x->tv_nsec) / 1000000000 + 1;
      y->tv_nsec -= 1000000000 * nsec;
      y->tv_sec += nsec;
    }
    if (x->tv_nsec - y->tv_nsec > 1000000000) {
      long nsec = (x->tv_nsec - y->tv_nsec) / 1000000000;
      y->tv_nsec += 1000000000 * nsec;
      y->tv_sec -= nsec;
    }

    /* Compute the time remaining to wait.
     *      tv_nsec is certainly positive. */
    result->tv_sec = x->tv_sec - y->tv_sec;
    result->tv_nsec = x->tv_nsec - y->tv_nsec;

    /* Return 1 if result is negative. */
    return x->tv_sec < y->tv_sec;
}

#define HOSTNAME_MAX 20

struct matching {
  int sender;
  int receiver;
};

static int compare_matching(const void *a, const void *b)
{
  const struct matching *aa = a;
  const struct matching *bb = b;
  if (aa->receiver > bb->receiver) return 1;
  if (aa->receiver < bb->receiver) return -1;
  return 0;
}

/*
 * This function should exchange forward and backward migration matrices.
 *
 * Current implementation creates all the matrices in rank 0 and sends
 * them around to other ranks.
 *
 * Forward migration matrix is a matrix of type rank -> set(rank). For
 * each rank it returns a set of ranks which reside on a node, where
 * the rank wants to migrate. It is used by the send part of migration
 * operation.
 *
 * Backward migration matrix is a matrix of type rank ->
 * set(rank). For each rank it return a set of ranks which want to
 * migrate to a node, where the rank is running. It is used by the
 * receive part of the migration operation.
 */
void exchange_forward_and_backward(struct work_item *new_schedule, int num_nodes)
{
  struct work_item *cur_schedule;
  size_t rank_vector_size = scr_ranks_world * sizeof(int);

  if (scr_my_rank_world == 0) {
    cur_schedule = SCR_MALLOC(scr_ranks_world * sizeof(*cur_schedule));
  }

  int node_id;
  int rank_in_node;

  MPI_Comm_rank(scr_comm_node_across, &node_id);
  MPI_Bcast(&node_id, 1, MPI_INT, 0, scr_comm_node);

  struct work_item cur_schedule_item = {
    .id = scr_my_rank_world,
    .node = node_id,
    .work = 0
  };

  MPI_Gather(&cur_schedule_item, sizeof(cur_schedule_item), MPI_BYTE,
             cur_schedule, sizeof(cur_schedule_item), MPI_BYTE,
             0, scr_comm_world);

  int cur_match;
  struct matching * matching;

  int *forward, *forward_len;
  int *node_to_rank, *node_to_rank_len;

  int *rank_to_node_cur;
  int *rank_to_node_new;

  if (scr_my_rank_world == 0) {
    qsort(cur_schedule, scr_ranks_world, sizeof(*cur_schedule), compare_work_item_node);
    qsort(new_schedule, scr_ranks_world, sizeof(*new_schedule), compare_work_item_node);

    rank_to_node_cur = (int *)SCR_MALLOC(scr_ranks_world*sizeof(int));
    for (int i = 0; i < scr_ranks_world; i++)
      rank_to_node_cur[cur_schedule[i].id] = cur_schedule[i].node;

    rank_to_node_new = (int *)SCR_MALLOC(scr_ranks_world*sizeof(int));
    for (int i = 0; i < scr_ranks_world; i++)
      rank_to_node_new[new_schedule[i].id] = new_schedule[i].node;

    cur_match = 0;
    matching = (struct matching *)SCR_MALLOC(scr_ranks_world*sizeof(*matching));

    int cur_recv = -1, next_node = -1, node_start = 0;
    for (int i = 0; i < scr_ranks_world; i++) {
      if (rank_to_node_new[new_schedule[i].id] == rank_to_node_cur[new_schedule[i].id])
        continue;

      /* We need to update pointers for exchange */
      if (new_schedule[i].node == cur_schedule[cur_recv + 1].node) {
        cur_recv ++;

        if (cur_schedule[cur_recv].node != cur_schedule[cur_recv - 1].node)
          node_start = cur_recv;

      } else if (new_schedule[i].node > cur_schedule[cur_recv + 1].node) {
        /* Roll forward */
        if (next_node > cur_recv) {
          cur_recv = next_node;
        }

        while (cur_schedule[cur_recv].node < new_schedule[i].node)
          cur_recv++;
        node_start = cur_recv;
      } else {
        /* Roll backward */
        cur_recv = node_start;
      }

      assert(new_schedule[i].node == cur_schedule[cur_recv].node);

      matching[cur_match].sender = new_schedule[i].id;
      matching[cur_match].receiver = cur_schedule[cur_recv].id;

      cur_match++;
    }

    /* Now we tell the senders where to send. If a rank gets -1, it does not send */

    forward = SCR_MALLOC(scr_ranks_world*sizeof(*forward));

    for (int i = 0; i < scr_ranks_world; i++)
      forward[i] = -1;
    for (int i = 0; i < cur_match; i++)
      forward[matching[i].sender] = matching[i].receiver;
  }

  int receiver;
  MPI_Scatter(forward, 1, MPI_INT,
              &receiver, 1, MPI_INT,
              0, scr_comm_world);

  int senders_len, *send_count, *displs;
  struct matching *senders, *scattered_matching;
  int max_count = 0;

  if (scr_my_rank_world == 0) {
    send_count = SCR_MALLOC(scr_ranks_world*sizeof(*send_count));
    displs = SCR_MALLOC(scr_ranks_world*sizeof(*displs));
    /* Now we tell the receivers who is going to send them */
    qsort(matching, cur_match, sizeof(*matching), compare_matching);

    for (int i = 0; i < scr_ranks_world; i++) {
      send_count[i] = 0;
    }

    {
      int i = 0;
      while (i < cur_match) {
        int count = 0;
        do {
          count++;
          i++;
        } while((i < cur_match) &&
                (matching[i - 1].receiver == matching[i].receiver));
        if (count > max_count)
          max_count = count;
        send_count[matching[i-1].receiver] = count * sizeof(struct matching);
      }
    }

    scattered_matching = (struct matching *)SCR_MALLOC(max_count * scr_ranks_world * sizeof(struct matching));

    /* XXX: TODO for Maksym rewrite this */

    displs[0] = 0;
    for (int i = 1; i < scr_ranks_world; i++) {
      displs[i] = displs[i-1] + send_count[i-1];
    }

    for (int i = 0; i < scr_ranks_world; i++) {
      for (int j = 0; j < max_count; j++) {
        scattered_matching[i*max_count + j] = matching[displs[i] / sizeof(struct matching) + j];
      }
    }
  }


  MPI_Bcast(&max_count, 1, MPI_INT, 0, scr_comm_world);
  MPI_Scatter(send_count, 1, MPI_INT,
              &senders_len, 1, MPI_INT,
              0, scr_comm_world);

  senders = (struct matching *)SCR_MALLOC(max_count*sizeof(senders));

  MPI_Scatter(scattered_matching, max_count * sizeof(struct matching), MPI_BYTE,
              senders, max_count * sizeof(struct matching), MPI_BYTE,
              0, scr_comm_world);

  scr_reddesc_migration *state = scr_balance_reddesc_migration();

  assert(state->backward == NULL);
  assert(state->backward_count == 0);

  senders_len /= sizeof(struct matching);

  if (senders_len > 0) {
    int *backward = (int *)SCR_MALLOC(senders_len * sizeof(*backward));
    for (int i = 0; i < senders_len; i++) {
      assert(senders[i].receiver == scr_my_rank_world);
      backward[i] = senders[i].sender;
    }
    qsort(backward, senders_len, sizeof(*backward), compare_int);
    state->backward = backward;
    state->backward_count = senders_len;
  } else {
    state->backward = NULL;
    state->backward_count = 0;
  }

  state->forward = receiver;

  scr_free(&senders);

  if (scr_my_rank_world == 0) {
    scr_free(&displs);
    scr_free(&send_count);
    scr_free(&forward);
    scr_free(&matching);
    scr_free(&rank_to_node_new);
    scr_free(&rank_to_node_cur);
    scr_free(&cur_schedule);
    scr_free(&scattered_matching);
  }
}

int dump_schedule(struct work_item *chunks, int processes, int num_nodes)
{
  int rank_node;
  MPI_Comm_rank(scr_comm_node, &rank_node);

  if (rank_node == 0) {
    int rank_across;

    MPI_Comm_rank(scr_comm_node_across, &rank_across);

    char *nodenames = NULL;
    if (rank_across == 0) {
      nodenames = (char *)SCR_MALLOC(num_nodes * HOSTNAME_MAX);
      memset(nodenames, 0, num_nodes * HOSTNAME_MAX);
    }

    char my_hostname[HOSTNAME_MAX];
    gethostname(my_hostname, HOSTNAME_MAX);
    /* TODO for Maksym: Do I need to do this? */
    /* For safety */
    my_hostname[HOSTNAME_MAX-1] = '\0';

    MPI_Gather(my_hostname, HOSTNAME_MAX, MPI_CHAR, nodenames, HOSTNAME_MAX, MPI_CHAR, 0,
        scr_comm_node_across);

    if (rank_across == 0) {
      int fd = -1;
      mode_t mode_file = scr_getmode(1, 1, 0);
      char *file = NULL;

      file = scr_path_strdup(scr_balancer_file);

      fd = scr_open(file, O_WRONLY | O_CREAT, mode_file);
      if (fd < 0) {
        scr_err("Opening file for write: scr_open(%s) errno=%d %s @ %s:%d",
            file, errno, strerror(errno), __FILE__, __LINE__);
        goto cleanup;
      }

      /* acquire an exclusive file lock before reading */
      if (scr_file_lock_write(file,fd) != SCR_SUCCESS) {
        goto cleanup;
      }


      qsort(chunks, scr_ranks_world, sizeof(*chunks), compare_work_item_id);

      for (int i = 0; i < processes; i++) {
        /* 2 characters for \n and 1 character for \O. Not sure if this right calculation */
        char line[HOSTNAME_MAX+3];

        snprintf(line, HOSTNAME_MAX + 3, "%s\n", &nodenames[chunks[i].node*HOSTNAME_MAX]);
        write(fd, line, strlen(line));
      }

      /* release the file lock */
      if (scr_file_unlock(file, fd)!= SCR_SUCCESS) {
        goto cleanup;
      }

cleanup:

      if (fd >= 0)
        scr_close(file, fd);
      scr_free(&nodenames);
      scr_free(&file);
    }
  }
  return 0;
}

static void propose_schedule(double time, int num_nodes, double measured_imbalance)
{
  MPI_Status status;
  MPI_Request request;
  double *schedule;
  struct work_item my_item;
  struct work_item *chunks;

  int work_item_size;
  if (scr_my_rank_world == 0) {
    chunks = (struct work_item*)SCR_MALLOC(scr_ranks_world * sizeof(*chunks));
  }

  if (scr_balancer_debug) {
    int rc;
    double *current_time;
    if (scr_my_rank_world == 0) {
      current_time = (double *)SCR_MALLOC(scr_ranks_world * sizeof(double));
    }

    rc = MPI_Gather(&time, 1, MPI_DOUBLE,
                    current_time, 1, MPI_DOUBLE, 0,
                    scr_comm_world);
    if (rc != MPI_SUCCESS) {
      scr_abort(-1, "Failed to gather time data to debugging dump @ %s:%d",
                __FILE__, __LINE__);
    }

    if (scr_my_rank_world == 0) {
      FILE *file;
      char filename[SCR_MAX_FILENAME];
      snprintf(filename, SCR_MAX_FILENAME, "%s.%d", scr_balancer_debug, scr_ranks_world);

      file = fopen(filename, "a");
      fwrite(&scr_ranks_world, sizeof(scr_ranks_world), 1, file);
      fwrite(current_time, sizeof(double), scr_ranks_world, file);
      fclose(file);
      scr_free(&current_time);
    }

  }

  my_item.work = time;
  my_item.id = scr_my_rank_world;
  my_item.node = -1;
  MPI_Aint lb, intex = 0;
  MPI_Barrier(scr_comm_world);
  MPI_Type_get_extent(MPI_WORK_ITEM, &lb, &intex);
  MPI_Gather(&my_item, 1, MPI_WORK_ITEM, chunks, 1, MPI_WORK_ITEM, 0,
      scr_comm_world);
  // TODO for Maksym: I want to use Igather here
  //MPI_Igather(&my_item, 1, MPI_WORK_ITEM, chunks, 1, MPI_WORK_ITEM, 0,
  //    scr_comm_world, &request);

  if (scr_my_rank_world == 0) {
    scr_balance_timestamp_nb("ALGORITHM_START");
    schedule = SCR_MALLOC(num_nodes * sizeof(*schedule));
    memset(schedule, 0, num_nodes * sizeof(*schedule));

    qsort(chunks, scr_ranks_world, sizeof(*chunks), compare_work_item_work);

    struct work_item **per_id_chunks;
    int *node_list;
    int free_nodes;
    per_id_chunks= SCR_MALLOC(num_nodes * sizeof(*per_id_chunks));
    node_list = SCR_MALLOC(num_nodes * sizeof(*node_list));

    for (int i = 0; i < scr_ranks_world / num_nodes; i++) {
      /* The idea is to work around somewhat broken XOR groups, where
         each node should be in a separate XOR group. */

      /* For each local id */

      /* Sort ranks with the same local ids */
      int offset = scr_ranks_world / num_nodes;
      for (int j = 0; j < num_nodes; j++)
        per_id_chunks[j] = &chunks[i+j*offset];
      qsort(per_id_chunks, num_nodes, sizeof(*per_id_chunks), compare_work_item_ptr);

      /* Initialize node occupation list */
      free_nodes = num_nodes;
      for (int j = 0; j < free_nodes; j++)
        node_list[j] = j;

      /* For each rank with specific local id */
      for (int j = 0; j < num_nodes; j++) {
        int min_node = 0;
        for (int k = 0; k < free_nodes; k++) {
          if (schedule[node_list[k]] < schedule[node_list[min_node]])
            min_node = k;
        }

        /* Put the chunk on the node with least workload among
           untouched ones in this cycle*/
        per_id_chunks[j]->node = node_list[min_node];
        /* Record change in the schedule */
        schedule[node_list[min_node]] += per_id_chunks[j]->work;

        /* Decrement number of untouched nodes */
        free_nodes -= 1;
        /* Replace the last node in the list for the just allocated one. */
        node_list[min_node] = node_list[free_nodes];
      }
    }

    scr_free(&per_id_chunks);
    scr_free(&node_list);

    double max = 0.;
    double avg = 0.;
    /* Compute predicted imbalance */
    for (int i = 0; i < num_nodes; i ++) {
      if (schedule[i] > max) {
        max = schedule[i];
      }
      avg += schedule[i];
    }
    avg /= num_nodes;
    double imbalance = max / avg;
    scr_err("I predict imbalance of %f", imbalance);

    if (measured_imbalance > scr_imbalance_threshold &&
        scr_imbalance_threshold > imbalance && !scr_balancer_dry_run)
      scr_balancer_do_migrate = 1;
    else {
      scr_balancer_do_migrate = 0;
    }

    scr_free(&schedule);
    scr_balance_timestamp_nb("ALGORITHM_END");
  }

  MPI_Bcast(&scr_balancer_do_migrate, 1, MPI_INT, 0, scr_comm_world);

  if (scr_balancer_do_migrate) {
    scr_balance_reddesc_set_chunks(chunks);
    scr_balance_timestamp("FORWARD_START");
    exchange_forward_and_backward(chunks, num_nodes);
    scr_balance_timestamp("FORWARD_END");
    scr_balance_timestamp("DUMP_START");
    dump_schedule(chunks, scr_ranks_world, num_nodes);
    scr_balance_timestamp("DUMP_END");
  } else if (scr_my_rank_world == 0) {
    /* Chunks are not needed anymore */
    scr_free(&chunks);
  }
}

static double calculate_imbalance(double time)
{
  double max, avg, sum;
  double imbalance;
  int rank_node;

  MPI_Comm_rank(scr_comm_node, &rank_node);
  MPI_Reduce(&time, &sum, 1, MPI_DOUBLE, MPI_SUM, 0, scr_comm_node);

  int ranks_across;
  MPI_Comm_size(scr_comm_node_across, &ranks_across);

  int num_nodes;
  MPI_Allreduce(&ranks_across, &num_nodes, 1, MPI_INT, MPI_MAX, scr_comm_world);

  if (rank_node == 0) {

    int num_req = 0;
    MPI_Status status[2];
    MPI_Request request[2];

    MPI_Ireduce(&sum, &max, 1, MPI_DOUBLE, MPI_MAX, 0, scr_comm_node_across, &request[num_req++]);
    MPI_Ireduce(&sum, &avg, 1, MPI_DOUBLE, MPI_SUM, 0, scr_comm_node_across, &request[num_req++]);
    MPI_Waitall(num_req, request, status);

    avg /= num_nodes;

    imbalance = max / avg;
   // scr_err("I'm leader of node %d and I my sum is %f", node_id, sum);
    if (scr_my_rank_world == 0) {
      scr_err("I see imbalance of %f", max/avg);
    }
  }

  propose_schedule(time, num_nodes, imbalance);

  return imbalance;
}

int scr_balance_need_checkpoint(int *flag)
{
  if (!scr_balancer) {
    scr_err("Balancer is off");
    return 0;
  }

  scr_reddesc *reddesc = scr_reddesc_for_migration(scr_nreddescs, scr_reddescs);

  if (!reddesc->enabled) {
    scr_abort(-1, "SCR balancer reddesc is disabled."
              " Migration is not possible @ %s:%d",
              __FILE__, __LINE__);
    return SCR_FAILURE;
  }

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_abort(-1, "SCR has not been initialized @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  struct rusage my_rusage;
  struct timespec cur_step;
  struct timeval *utimes;

  getrusage(RUSAGE_SELF, &my_rusage);
  clock_gettime(CLOCK_MONOTONIC, &cur_step);

  scr_balance_timestamp("NEED_CHECKPOINT_ENTER");

  if (last_step.tv_sec == 0 && last_step.tv_nsec == 0) {
    /* Making first time step, need previous record */
    last_step.tv_sec = cur_step.tv_sec;
    last_step.tv_nsec = cur_step.tv_nsec;
    last_timeval.tv_sec = my_rusage.ru_utime.tv_sec;
    last_timeval.tv_usec = my_rusage.ru_utime.tv_usec;
    return SCR_SUCCESS;
  }

  /* Convert time from struct timeval to double to be able to perform
   * a reduce operation using MPI without defining own datatype
   * operation*/
  double time = (cur_step.tv_sec - last_step.tv_sec) * SEC
                + (cur_step.tv_nsec - last_step.tv_nsec) * NSEC;
  time = my_rusage.ru_utime.tv_sec * SEC + my_rusage.ru_utime.tv_usec * USEC;
  time = (my_rusage.ru_utime.tv_sec - last_timeval.tv_sec) * SEC
                + (my_rusage.ru_utime.tv_usec - last_timeval.tv_usec) * USEC;
  double imbalance;

  static char hostname[MPI_MAX_PROCESSOR_NAME];
  int namelen;
  MPI_Get_processor_name(hostname, &namelen);

  scr_balance_timestamp("DECISION_START");

  imbalance = calculate_imbalance(time);

  scr_balance_timestamp("DECISION_END");

  if (scr_balancer_do_migrate) {
    *flag = 1;
  }

  return SCR_SUCCESS;
}

int scr_balance_complete_checkpoint(int valid)
{
  struct rusage my_rusage;
  struct timespec cur_step;

  /* Need to measure again to ignore time spent in balancer and checkpointing */
  getrusage(RUSAGE_SELF, &my_rusage);
  clock_gettime(CLOCK_MONOTONIC, &cur_step);

  /* Remember the beginning of next interval (timestep) */
  last_step.tv_sec = cur_step.tv_sec;
  last_step.tv_nsec = cur_step.tv_nsec;
  last_timeval.tv_sec = my_rusage.ru_utime.tv_sec;
  last_timeval.tv_usec = my_rusage.ru_utime.tv_usec;

  scr_balance_timestamp("BALANCE_COMPLETE_CHECKPOINT");

  return SCR_SUCCESS;
}
