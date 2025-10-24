#!/bin/bash
set -uoe pipefail

# Total number of articles per media, per month
xan parallel cat \
  --progress \
  -S media \
  -B -1 \
  -P 'select -e "ym(date_published).try() || `N/A` as month" | freq -s month -A | select -e "value as month, count" | sort -s month' \
  */articles.csv.gz | \
xan transform media '_.split("/")[0]' > medias-month-totals.csv

# Running queries
xan parallel cat \
  -S media \
  -B -1 \
  --progress \
  -P '
    select -f scripts/harmonization.moonblade |
    search -Bri -s headline,description,text
      --patterns scripts/climate_week/queries.csv
      --pattern-column pattern
      --name-column name |
    map "date_published.ym().try() || `N/A` as month" |
    groupby month -C -5: "count(_ > 0)" |
    sort -s month' \
  */articles.csv.gz | \
xan transform media '_.split("/")[0]' > medias-month-matches.csv

# Joining totals
xan join --left media,month medias-month-totals.csv media,month medias-month-matches.csv | \
xan search -s month -v N/A | \
xan select media,month,count,query_* | \
xan rename -s count total | \
xan fill -s query_* -v 0 | \
xan sort -s media,month > medias-month-breakdown.csv

# Running total query
xan parallel cat \
  --progress \
  -S media \
  -B -1 \
  -P '
    select -f scripts/harmonization.moonblade |
    search -c query_total -ri -s headline,description,text
      --patterns scripts/climate_week/queries.csv
      --pattern-column pattern |
    map "date_published.ym().try() || `N/A` as month" |
    groupby month "count() as query_total" |
    sort -s month' \
  */articles.csv.gz | \
xan transform media '_.split("/")[0]' > medias-month-matches-totals.csv

# Joining totals
xan join --left media,month medias-month-totals.csv media,month medias-month-matches-totals.csv | \
xan search -s month -v N/A | \
xan select media,month,count,query_* | \
xan rename -s count total | \
xan fill -s query_* -v 0 | \
xan sort -s media,month > medias-month-breakdown-totals.csv
