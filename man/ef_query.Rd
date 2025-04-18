% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ef_query.R
\name{ef_query}
\alias{ef_query}
\title{ef query}
\usage{
ef_query(anchor_date_table = NULL, before = NULL, after = NULL)
}
\arguments{
\item{anchor_date_table}{a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.}

\item{before}{an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.}

\item{after}{an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.}
}
\value{
a data.table with the following columns:
person_id, measurement_date, ef
}
\description{
ef query
}
\examples{
/dontrun{
ef_dat <- ef_query()
}
}
