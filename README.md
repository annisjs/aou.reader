# Overview

aou.reader contains functions for common data pulls in the All of Us Researcher Workbench involving ICD9/10 codes, labs, medications, and surveys.

# Installation
In an R notebook on the AOU workbench:
```r
devtools::install_github("annisjs/aou.reader",upgrade=F)
```

# Example
```r
library(aou.reader)
icd9_dat <- icd9_codes(c("410","410.%"))
icd10_dat <- icd10_codes(c("I21","I21.%"))
