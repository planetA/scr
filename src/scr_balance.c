/*
 * Maksym Planeta, 2016
 */

#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <assert.h>
#include <stddef.h>

#include "scr_globals.h"

#define NSEC 1
#define USEC 1000*NSEC
#define MSEC 1000*USEC
#define  SEC 1000*MSEC

static struct timespec last_step;
static struct timeval last_timeval;
static MPI_Datatype MPI_WORK_ITEM = 0;
static int worth_to_migrate = 0;

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

#define MPI_Aint_diff(addr1, addr2) ((MPI_Aint) ((char *) (addr1) - (char *) (addr2)))

int scr_balance_init(void)
{
  memset(&last_step, 0, sizeof(last_step));

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
}

int scr_balance_finalize(void)
{
  MPI_Type_free(&MPI_WORK_ITEM);
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


static void propose_schedule(double time)
{
  MPI_Status status;
  MPI_Request request;
  double *schedule;
  struct work_item my_item;
  struct work_item *chunks;

  int work_item_size;
  if (scr_my_rank_world == 0) {
    chunks = SCR_MALLOC(scr_ranks_world * sizeof(*chunks));
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

  int nodes;
  if (scr_my_rank_world == 0) {
    MPI_Comm_size(scr_comm_node_across, &nodes);
    schedule = SCR_MALLOC(nodes * sizeof(*schedule));
    memset(schedule, 0, nodes * sizeof(*schedule));
  }

  //MPI_Wait(&request, &status);
  MPI_Barrier(scr_comm_world);
  MPI_Barrier(scr_comm_world);

  if (scr_my_rank_world == 0) {
    qsort(chunks, scr_my_rank_world, sizeof(*chunks), compare_work_item);

    for (int i = 0; i < scr_ranks_world; i++) {
      int min_node = 0;
      for (int j = 1; j < nodes; j++) {
        if (schedule[j] < schedule[min_node]) {
          min_node = j;
        }
      }
      schedule[min_node] += chunks[i].work;
      chunks[i].node = min_node;
    }

    double max = 0.;
    double avg = 0.;
    for (int i = 0; i < nodes; i ++) {
      if (schedule[i] > max) {
        max = schedule[i];
      }
      avg += schedule[i];
    }
    avg /= nodes;
    double imbalance = max / avg;
    scr_err("I predict imbalance of %f", imbalance);

    if (imbalance > 1.25)
      worth_to_migrate = 1;
    else
      worth_to_migrate = 0;

    scr_free(chunks);
  }
}

static double calculate_imbalance(double time)
{
  double max, avg, sum;
  double imbalance;
  int rank_node;

  MPI_Comm_rank(scr_comm_node, &rank_node);
  MPI_Reduce(&time, &sum, 1, MPI_DOUBLE, MPI_SUM, 0, scr_comm_node);

  if (rank_node == 0) {

    int nodes;
    int num_req = 0;
    MPI_Status status[2];
    MPI_Request request[2];

    MPI_Comm_size(scr_comm_node_across, &nodes);
    MPI_Ireduce(&sum, &max, 1, MPI_DOUBLE, MPI_MAX, 0, scr_comm_node_across, &request[num_req++]);
    MPI_Ireduce(&sum, &avg, 1, MPI_DOUBLE, MPI_SUM, 0, scr_comm_node_across, &request[num_req++]);
    MPI_Waitall(num_req, request, status);

    avg /= nodes;

    imbalance = max / avg;
    int node_id;
    MPI_Comm_rank(scr_comm_node_across, &node_id);
   // scr_err("I'm leader of node %d and I my sum is %f", node_id, sum);
    if (scr_my_rank_world == 0) {
      scr_err("I see imbalance of %f", max/avg);
    }
  }
  propose_schedule(time);
  return imbalance;
}

int scr_balance_need_checkpoint(int *flag)
{
  if (!scr_balancer) {
    scr_err("Balancer is off");
    return 0;
  }

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_abort(-1, "SCR has not been initialized @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  //scr_err("Enter LB");
  // fflush(stdout);

  struct rusage my_rusage;
  struct timespec cur_step;
  struct timeval *utimes;

  getrusage(RUSAGE_SELF, &my_rusage);
  clock_gettime(CLOCK_MONOTONIC, &cur_step);


  if (last_step.tv_sec == 0 && last_step.tv_nsec == 0) {
    /* Making first time step, need previous record */ 
    last_step.tv_sec = cur_step.tv_sec;
    last_step.tv_nsec = cur_step.tv_nsec;
    last_timeval.tv_sec = my_rusage.ru_utime.tv_sec;
    last_timeval.tv_usec = my_rusage.ru_utime.tv_usec;
    return 0;
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

  imbalance = calculate_imbalance(time);
  //scr_err("I'm %d run on %s for time %f", scr_my_rank_world, hostname, time);
  last_step.tv_sec = cur_step.tv_sec;
  last_step.tv_nsec = cur_step.tv_nsec;
  last_timeval.tv_sec = my_rusage.ru_utime.tv_sec;
  last_timeval.tv_usec = my_rusage.ru_utime.tv_usec;
}
