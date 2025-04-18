% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/daily_heart_rate_query.R
\name{daily_heart_rate_query}
\alias{daily_heart_rate_query}
\title{Daily heart rate query}
\usage{
daily_heart_rate_query(
  anchor_date_table = NULL,
  before = NULL,
  after = NULL,
  cohort = NULL
)
}
\arguments{
\item{anchor_date_table}{a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.}

\item{before}{an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.}

\item{after}{an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.}

\item{cohort}{a vector or list of integers indicating the person_id to be queried.}
}
\value{
a data.table with the following columns:
person_id, date, mean_daily_heart_rate, sd_daily_heart_rate
}
\description{
Daily heart rate query
}
\details{
Returns the average and standard deviation of heart rate at each day.
}
\examples{
\dontrun{
hr_df <- daily_heart_rate_query()
}
}
