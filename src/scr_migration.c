/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
 *
 * Maksym Planeta, TU Dresden, 2016
*/

/* This is a utility program that checks various conditions in the halt
 * file to determine whether the job is in the process of migration. */

#include "scr.h"
#include "scr_io.h"
#include "scr_path.h"
#include "scr_util.h"
#include "scr_err.h"
#include "scr_hash.h"
#include "scr_hash_util.h"
#include "scr_param.h"
#include "scr_halt.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <getopt.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/file.h>

/* gettimeofday */
#include <sys/time.h>

#define PROG ("scr_migration")
#define NAME (".scr/halt.scr")
#define DO_MIGRATION (0)
#define NO_MIGRATION (1)

int print_usage()
{
  printf("\n");
  printf("  Usage:  %s --dir <dir>\n", PROG);
  printf("\n");
  exit(1);
}

struct arglist {
  char* dir; /* direcotry containing halt file */
};

int process_args(int argc, char **argv, struct arglist* args)
{
  /* define our options */
  static struct option long_options[] = {
    {"dir",       required_argument, NULL, 'd'},
    {"help",      no_argument,       NULL, 'h'},
    {0, 0, 0, 0}
  };

  /* set our options to default values */
  args->dir = NULL;

  /* loop through and process all options */
  int c;
  do {
    /* read in our next option */
    int option_index = 0;
    c = getopt_long(argc, argv, "d:h", long_options, &option_index);
    switch (c) {
      case 'd':
        /* directory containing halt file */
        args->dir = optarg;
        break;
      case 'h':
        /* print help message and exit */
        print_usage();
        break;
      case '?':
        /* getopt_long printed an error message */
        break;
      default:
        if (c != -1) {
          /* missed an option */
          scr_err("%s: Option '%s' specified but not processed", PROG, argv[option_index]);
        }
    }
  } while (c != -1);

  /* check that we got a directory name */
  if (args->dir == NULL) {
    scr_err("%s: Must specify directory containing halt file via '--dir <dir>'", PROG);
    return 0;
  }

  return 1;
}

/* read in halt file (which program may have changed), update internal data structure,
 * set & unset any fields, and write out halt file all while locked */
int scr_halt_sync_and_set(const scr_path* file_path, struct arglist* args, scr_hash* data)
{
  /* convert path to string */
  char* file = scr_path_strdup(file_path);
  /* Assume no migration case */
  int rc = NO_MIGRATION;
  int fd = -1;

  /* if we don't have a halt file, we're ok to continue */
  if (scr_file_exists(file) != SCR_SUCCESS) {
    printf("%s: CONTINUE RUN: No halt file found.\n", PROG);
    rc = NO_MIGRATION;
    goto cleanup;
  }

  /* TODO: sleep and try the open several times if the first fails */
  /* open the halt file for reading */
  mode_t mode_file = scr_getmode(1, 1, 0);
  fd = scr_open(file, O_RDWR | O_CREAT, mode_file);
  if (fd < 0) {
    scr_err("Opening file for write: scr_open(%s) errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );
    rc = NO_MIGRATION;
    goto cleanup;
    /* restore the normal file mask */
  }

  /* acquire an exclusive file lock before reading */
  if (scr_file_lock_write(file,fd) != SCR_SUCCESS) {
    rc = NO_MIGRATION;
    goto cleanup;
  }

  /* read in the current data from the file */
  scr_hash_read_fd(file, fd, data);

  char* value = NULL;
  /* check whether a reason has been specified */
  if (scr_hash_util_get_str(data, SCR_HALT_KEY_EXIT_REASON, &value) == SCR_SUCCESS) {
    if (strcmp(value, "MIGRATION") == 0) {
      printf("%s: HALT RUN: Reason: %s.\n", PROG, value);
      scr_hash_set_kv(data, SCR_HALT_KEY_EXIT_REASON, "AFTER_MIGRATION");
      rc = DO_MIGRATION;
    }
  }
  /* wind file pointer back to the start of the file */
  lseek(fd, 0, SEEK_SET);

  /* write our updated data */
  ssize_t bytes_written = scr_hash_write_fd(file, fd, data);

  /* truncate the file to the correct size (may be smaller than it was before) */
  if (bytes_written >= 0) {
    ftruncate(fd, (off_t) bytes_written);
  }

  /* release the file lock */
  if (scr_file_unlock(file, fd)!= SCR_SUCCESS) {
    rc = NO_MIGRATION;
    goto cleanup;
  }

cleanup:
  /* close file */
  if (fd >= 0)
    scr_close(file, fd);

  /* free file name string */
  scr_free(&file);

  /* write current values to halt file */
  return rc;
}

void scr_balance_timestamp(const char *message)
{
  int scr_my_rank_world;
  char* value = NULL;
  if ((value = getenv("SLURM_PROCID")) != NULL) {
    scr_my_rank_world = atoi(value);

    if (scr_my_rank_world == 0) {
      /* Print current time in milliseconds to log the work */
      struct timespec cur_step;
      clock_gettime(CLOCK_REALTIME, &cur_step);
      long long unsigned time = cur_step.tv_sec * 1000 + cur_step.tv_nsec / 1000000;
      printf("SCR_BALANCER_TOKEN: %s: %llu\n", message, time);
    }
  }
}

/* returns 0 if we need to halt, returns 1 otherwise */
int main (int argc, char *argv[])
{
  /* process command line arguments */
  struct arglist args;
  if (!process_args(argc, argv, &args)) {
    /* failed to process command line, to be safe, assume we need to migrate */
    return NO_MIGRATION;
  }

  scr_balance_timestamp("ENTER_SCR_MIGRATION");

  /* TODO: hopefully we don't abort right here and exit with wrong return code */
  /* create path to halt file */
  scr_path* halt_file = scr_path_from_str(args.dir);
  scr_path_append_str(halt_file, NAME);

  /* create a new hash to hold the file data */
  scr_hash* scr_halt_hash = scr_hash_new();

  /* otherwise, assume that we don't need to migrate, and check for valid condition */
  int rc = scr_halt_sync_and_set(halt_file, &args, scr_halt_hash);

  /* delete the hash holding the halt file data */
  scr_hash_delete(&scr_halt_hash);

  /* free off our file name storage */
  scr_path_delete(&halt_file);

  scr_balance_timestamp("LEAVE_SCR_MIGRATION");
  /* return appropriate exit code */
  return rc;
}

