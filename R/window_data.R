window_data <- function(dat,date_column,anchor_date_table,before,after)
{
    if (!is.null(anchor_date_table))
    {
        if (is.null(before))
        {
            before <- -100000
        }
        if (is.null(after))
        {
            after <- 100000
        }
        dat <- data.table::as.data.table(merge(dat,anchor_date_table,by="person_id",allow.cartesian=TRUE))
        dat[,min_window_date := anchor_date + before]
        dat[,max_window_date := anchor_date + after]
        dat <- dat[get(date_column) >= min_window_date]
        dat <- dat[get(date_column) <= max_window_date]
        dat[, min_window_date := NULL]
        dat[, max_window_date := NULL]
        dat[, anchor_date := NULL]
    }
    return(dat)
}