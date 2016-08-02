/*
 * Maksym Planeta, 2016
 */

#ifndef SCR_BALANCE_H
#define SCR_BALANCE_H

/* Callback to balancer from SCR_Need_checkpoint. This function may
   change the flag, and hence change the decision to make a
   checkpoint. */
int scr_balance_need_checkpoint(int *flag);

/* Callback to balancer from SCR_Complete_checkpoint */
int scr_balance_complete_checkpoint(int valid);

int scr_balance_init(void);
int scr_balance_finalize(void);
int scr_balance_finalize_promise(void);

/* Emit timestamp for performance measurements */
void scr_balance_timestamp(const char *message);
void scr_balance_timestamp_nb(const char *message);

#define scr_stat_emit(x) __scr_stat_emit(#x, x)
void __scr_stat_emit(const char *message, int64_t value);

#endif /* SCR_BALANCE_H */
