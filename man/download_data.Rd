% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/download_data.R
\name{download_data}
\alias{download_data}
\title{Download data given query}
\usage{
download_data(query, page_size = NULL)
}
\arguments{
\item{query}{a SQL query string}

\item{page_size}{The number of rows requested per chunk}
}
\value{
a data.table
}
\description{
Download data given query
}
