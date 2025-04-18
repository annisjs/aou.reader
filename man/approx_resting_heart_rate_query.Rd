% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/approx_resting_heart_rate_query.R
\name{approx_resting_heart_rate_query}
\alias{approx_resting_heart_rate_query}
\title{Approximate resting heart rate query}
\usage{
approx_resting_heart_rate_query(
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
}
\value{
a data.table with the following columns:
person_id, date, approx_resting_heart_rate
}
\description{
Approximate resting heart rate query
}
\details{
Looks for HR when there is no significant steps/movemet for 10 mins, and then takes 10th percentile of that set of observed values.
}
\examples{
/dontrun{
hr_dat <- approx_resting_heart_rate_query()
}
}
