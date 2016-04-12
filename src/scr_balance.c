/*
 * Maksym Planeta, 2016
 */

#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <assert.h>
#include <stddef.h>
#include <stdlib.h>

#include "scr_globals.h"

#define NSEC 1
#define USEC 1000*NSEC
#define MSEC 1000*USEC
#define  SEC 1000*MSEC

#define USE_MPI_IO 0

static struct timespec last_step;
static struct timeval last_timeval;
static MPI_Datatype MPI_WORK_ITEM = 0;
#if USE_MPI_IO
static MPI_File time_log_fh;
#endif

/*
 * Comparison function for qsort to sort doubles in descending order.
 */

struct work_item {
  int id;
  int node;
  double work;
};

static int compare_work_item(const void *a, const void *b) {
  const struct work_item *aa = a;
  const struct work_item *bb = b;
  if (aa->work > bb->work) return -1;
  if (aa->work < bb->work) return 1;
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
    clock_gettime(CLOCK_MONOTONIC, &cur_step);
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
    clock_gettime(CLOCK_MONOTONIC, &cur_step);
    long long unsigned time = cur_step.tv_sec * 1000 + cur_step.tv_nsec / 1000000;
    printf("SCR_BALANCER_TOKEN: %s: %llu\n", message, time);
  }
}

#define MPI_Aint_diff(addr1, addr2) ((MPI_Aint) ((char *) (addr1) - (char *) (addr2)))

int scr_balance_init(void)
{

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

    MPI_Aint size;
    MPI_Get_address((&sample)+1, &size);
    size = MPI_Aint_diff(size, base);
#undef NITEMS
  }

  /* Generate random number for matching algorithm. */
  srand(time(NULL) + scr_my_rank_world);
  srand(1 + scr_my_rank_world);

#if USE_MPI_IO
  if (scr_balancer_debug) {
    int rc;
    int amode = (MPI_MODE_APPEND | MPI_MODE_CREATE |
                 MPI_MODE_WRONLY);
    MPI_Info info = MPI_INFO_NULL;

    char file[SCR_MAX_FILENAME];
    snprintf(file, SCR_MAX_FILENAME, "/scratch/s9951545/scr_balancer.%d.debug", scr_ranks_world);
    rc = MPI_File_open(MPI_COMM_WORLD, file, amode, info, &time_log_fh);
    if (rc != MPI_SUCCESS) {
      scr_abort(-1, "Failed to open file %s @ %s:%d", file,
                __FILE__, __LINE__);
    }

    rc = MPI_File_set_view(time_log_fh, 0,
                      MPI_DOUBLE, MPI_DOUBLE, "native",
                      info);
    if (rc != MPI_SUCCESS) {
      scr_abort(-1, "Failed to set view for file @ %s:%d",
                __FILE__, __LINE__);
    }
  }
#endif

}

int scr_balance_finalize(void)
{
  MPI_Type_free(&MPI_WORK_ITEM);

#if USE_MPI_IO
  if (scr_balancer_debug) {
    MPI_File_close(&time_log_fh);
  }
#endif
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
  int *current_schedule;
  size_t rank_vector_size = scr_ranks_world * sizeof(int);

  if (scr_my_rank_world == 0) {
    current_schedule = SCR_MALLOC(scr_ranks_world * sizeof(int));
  }

  int node_id;
  int rank_in_node;

  MPI_Comm_rank(scr_comm_node_across, &node_id);
  MPI_Bcast(&node_id, 1, MPI_INT, 0, scr_comm_node);
  MPI_Gather(&node_id, 1, MPI_INT,
             current_schedule, 1, MPI_INT,
             0, scr_comm_world);

  int *forward, *forward_len;
  int *node_to_rank, *node_to_rank_len;
  if (scr_my_rank_world == 0) {
    /* Generate forward matrix */
    /* TODO for Maksym: requires huge amount of memory on large
       scale. Do not expect to work efficiently more than with 1000
       Processes */
    /* Size of forward and backward matrices is simple:
     *     N_ranks * N_ranks
     *
     * Explanation: At most each rank wants to migrate. At most to all
     * ranks are on the same node.
     */
    forward = (int *)SCR_MALLOC(rank_vector_size);
    forward_len = (int *)SCR_MALLOC(rank_vector_size);

    /* Shows the ranks which currently run on each node */
    node_to_rank = (int *)SCR_MALLOC(rank_vector_size * num_nodes);
    node_to_rank_len = (int *)SCR_MALLOC(sizeof(int) * num_nodes);
    memset(node_to_rank_len, 0, sizeof(int) * num_nodes);
    for (int i = 0; i < scr_ranks_world; i++) {
      int node_id = current_schedule[i];
      assert(node_id < num_nodes);

      int cur_len = node_to_rank_len[node_id]++;
      node_to_rank[scr_ranks_world * node_id + cur_len] = i;
    }

    for (int i = 0; i < scr_ranks_world; i++) {
      int rank_from = new_schedule[i].id;
      int new_node = new_schedule[i].node;

      assert((node_to_rank_len[new_node] > 0) &&
             (node_to_rank_len[new_node] < scr_ranks_world));

      int current_node = current_schedule[rank_from];
      if (new_node != current_node) {
        forward[rank_from] = new_node * scr_ranks_world;
        forward_len[rank_from] = node_to_rank_len[new_node];
      } else {
        forward[rank_from] = 0;
        forward_len[rank_from] = 0;
      }
    }
  }

  int my_forward_len;
  MPI_Scatter(forward_len, 1, MPI_INT,
              &my_forward_len, 1, MPI_INT,
              0, scr_comm_world);

  /* Add one to ensure the array is alwais allocated,
   * because my_forward_len is allowed to be 0. */
  int *my_forward = (int *)SCR_MALLOC((my_forward_len + 1) * sizeof(int));

  MPI_Scatterv(node_to_rank, forward_len, forward, MPI_INT,
               my_forward, my_forward_len, MPI_INT, 0, scr_comm_world);

  int my_partner = -1;

  if (my_forward_len)
    my_partner= my_forward[rand() % my_forward_len];


  int *backward;
  backward = (int *)SCR_MALLOC(rank_vector_size);

  MPI_Allgather(&my_partner, 1, MPI_INT,
                backward, 1, MPI_INT,
                scr_comm_world);

  int my_backward_count = 0;
  int *my_backward = NULL;

  for (int i = 0; i < scr_ranks_world; i++) {
    if (backward[i] == scr_my_rank_world) {
      my_backward_count++;
      /* I have to get a message from i */
      my_backward = (int *)SCR_REALLOC(my_backward, my_backward_count);
      my_backward[my_backward_count - 1] = i;
    }
  }

#if 0
  if (my_partner != -1)
    printf("Rank %5d sends a message to %d\n", scr_my_rank_world, my_partner);

  fflush(stdout);

  MPI_Barrier(MPI_COMM_WORLD);

  int m_len = 1024;
  char m_get[1024];
  int gets = 0;
  int offs = 0;
  snprintf(m_get + offs, m_len - offs, "Rank %5d gets a message from ", scr_my_rank_world);
  offs = strlen(m_get);

  for (int i = 0; i < scr_ranks_world; i++) {
    if (backward[i] == scr_my_rank_world) {
      snprintf(m_get + offs, m_len - offs, " %5d", i);
      offs = strlen(m_get);
      gets = 1;
    }
  }
  if (gets) {
    snprintf(m_get + offs, m_len - offs, "\n");
    printf(m_get);
  }
#endif

  scr_reddesc_migration *state = scr_balance_reddesc_migration();

  assert(state->backward == NULL);
  assert(state->backward_count == 0);

  state->forward = my_partner;
  state->backward = my_backward;
  state->backward_count = my_backward_count;

  scr_free(&my_forward);
  scr_free(&backward);

  if (scr_my_rank_world == 0) {
    scr_free(&forward);
    scr_free(&forward_len);
    scr_free(&node_to_rank);
    scr_free(&node_to_rank_len);
    scr_free(&current_schedule);
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
#if USE_MPI_IO
    MPI_Status status;

    time = (double)scr_my_rank_world;
    rc = MPI_File_write_ordered(time_log_fh, &time, 1, MPI_DOUBLE, &status);
    if (rc != MPI_SUCCESS) {
      scr_abort(-1, "Failed to write to file @ %s:%d",
                __FILE__, __LINE__);
    }

    int count;
    MPI_Get_count(&status, MPI_DOUBLE, &count);
    if (count != 1) {
      scr_abort(-1, "Wrong number of values has been written: %d @ %s:%d",
                count, __FILE__, __LINE__);
    }
#else
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
      snprintf(filename, SCR_MAX_FILENAME, scr_balancer_debug, scr_ranks_world);

      file = fopen(filename, "a");
      fwrite(&scr_ranks_world, sizeof(scr_ranks_world), 1, file);
      fwrite(current_time, sizeof(double), scr_ranks_world, file);
      fclose(file);
      scr_free(&current_time);
    }
#endif

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
    schedule = SCR_MALLOC(num_nodes * sizeof(*schedule));
    memset(schedule, 0, num_nodes * sizeof(*schedule));
  }

  //MPI_Wait(&request, &status);
  MPI_Barrier(scr_comm_world);
  MPI_Barrier(scr_comm_world);

  if (scr_my_rank_world == 0) {
    qsort(chunks, scr_ranks_world, sizeof(*chunks), compare_work_item);

#define ROUND_ROBIN 0
#define ROUND_ROLLING 1
#if ROUND_ROBIN
    int cur_node = 0;
#elif ROUND_ROLLING
    struct work_item **per_id_chunks;
    int *node_list;
    int free_nodes;
    per_id_chunks= SCR_MALLOC(scr_ranks_world / num_nodes * sizeof(*per_id_chunks));
    node_list = SCR_MALLOC(num_nodes * sizeof(*node_list));
#endif

    for (int i = 0; i < scr_ranks_world / num_nodes; i++) {
#if 0
      int min_node = 0;
      for (int j = 1; j < num_nodes; j++) {
        if (schedule[j] < schedule[min_node]) {
          min_node = j;
        }
      }
      schedule[min_node] += chunks[i].work;
      chunks[i].node = min_node;
#elif ROUND_ROBIN
      int offset = scr_ranks_world / num_nodes;
      for (int j = 0; j < num_nodes; j ++) {
        schedule[cur_node] += chunks[i+j*offset].work;
        chunks[i+j*offset].node = (cur_node + j) % num_nodes;
      }
      cur_node = (cur_node + 1) % num_nodes;
#elif ROUND_ROLLING
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
#endif
    }

#if ROUND_ROLLING
    scr_free(&per_id_chunks);
    scr_free(&node_list);
#endif

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

    if (measured_imbalance / imbalance > 1.10)
      scr_balancer_do_migrate = 1;
    else
      scr_balancer_do_migrate = 0;

  }

  MPI_Bcast(&scr_balancer_do_migrate, 1, MPI_INT, 0, scr_comm_world);

  if (scr_balancer_do_migrate) {
    scr_balance_reddesc_set_chunks(chunks);
    exchange_forward_and_backward(chunks, num_nodes);
    dump_schedule(chunks, scr_ranks_world, num_nodes);
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

scr_reddesc *scr_balancer_get_reddesc()
{
}
