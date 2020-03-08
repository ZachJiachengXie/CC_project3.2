#!/bin/bash

###############################################################################
##                 15-319/15-619 Cloud Computing Course                      ##
##               Runner Script for Project Social Network                    ##
##                                                                           ##
##     Copyright 2018-2019, 15-319/15-619: Cloud Computing Course            ##
##                     Carnegie Mellon University                            ##
##   Unauthorized distribution of copyrighted material, including            ##
##  unauthorized peer-to-peer file sharing, may subject the students         ##
##  to the fullest extent of Carnegie Mellon University copyright policies.  ##
###############################################################################

###############################################################################
##                      README Before You Start                              ##
###############################################################################
# Fill in the functions below for each question with the correct choice(s)
# in the function.
#
# This task grades your understanding of the differences between SQL and NoSQL
# databases, including which database best applies in which scenario. Please
# read the comments carefully. You are free to go through the primer(s) and the
# writeup(s) to figure out the answers to these questions.
#
# The colon `:` is a POSIX built-in basically equivalent to the `true` command,
# REPLACE it with your own command in each function.
# Before you fill your solution,
# DO NOT remove the colon or the function will break because the bash functions
# may not be empty!

###############################################################################
##                             Task 1                                        ##
###############################################################################
# In each of the following multiple choice questions, you should echo the
# correct option to StdOut. Please note there is a single correct choice and
# none of the questions require you to echo more than one choice.
# e.g. echo "A"

q1() {
  # Question:
  # Which of the following are advantages of NoSQL databases (such as Neo4j)
  # but are NOT of SQL databases such as MySQL.
  #
  # A. Superior non-sequential read/write performance over Flat Files
  # B. Dynamic Schema
  # C. Support for Indexing
  # D. Support for a Java API
  :
}

################################################################################
##                          Task 2                                            ##
################################################################################
# Read Carefully:
#
# The following questions present you with various scenarios in which database
# and storage choices need to be made. For each scenario, you are presented with 
# data needed to make the decision.
# Please select the most appropriate choice in each scenario.
#
# For questions asking you to select a single choice, print the
# character corresponding to the choice. For e.g., If the database corresponding
# to choice "A" is appropriate:
# echo "A"
#
# For questions asking you to pick multiple choices, print "ALL"
# correct choices as a SINGLE NUMBER, without spaces or commas. For e.g.
# If choice "A" and "B" are appropriate:
# echo "AB"

q2() {
  # SCENARIO:
  # We are starting a new photo-sharing platform, and our forecast indicates 
  # that all the content created on our platform will be popular or fresh for a
  # period of one week from the date of creation. After a week, the popularity 
  # of the content drops by 50 times and remains constant thereafter. 
  # If you are the CTO of the company, which data storage system would you 
  # suggest that our company should use? 
  # (Single Choice)
  #
  # A. A hot BLOB storage system with optimized IOPS to store all data. 
  # B. A graph store like Neo4j.
  # C. A combination of a hot BLOB storage system with optimized IOPS and a 
  #    warm storage system with estimated max IOPS. 
  # D. A warm storage system with a max IOPS of 20 for 1TB of data to store all
  #    the data. 
  : 
}

q3() {
  # SCENARIO:
  # You have been hired as a consultant by Twitter to assess its growing user
  # base and increasing diversity of content(Photos, GIF).
  # Which databases and storage systems would you suggest that Twitter should
  # use? 
  # (Choose all that apply)
  #
  # A. MySQL database to store tweets and unstructured textual data. 
  # B. A hot storage system with optimized IOPS to store BLOB content. 
  # C. Neo4j to store relationships between users. 
  # D. A warm storage system to store the most popular content.  
  :
}

q4() {
  # SCENARIO:
  # Consider a system where each server typically responds in 10ms but with a
  # 99th-percentile latency of one second. If a user request is handled on just
  # one such server, one user request in 100 will take 1 second (tail latency). 
  #
  # Consider a system wide fan-out, where each user request must collect
  # responses from 100 such servers in parallel, then how many of the user
  # requests in percentage terms will take more than one second?
  # Hint: 99 of the 100 requests take 10ms on each node, one node will take 
  # 1 sec. What’s the probability that a request, which needs to access 100   
  # nodes, takes 1 second? 
  # (Single Choice)
  #
  # A. Only 1% of user requests take more than 1 second. 
  # B. At most 36% of user requests take more than 1 second.
  # C. More than 63% of user requests take more than 1 second.
  # D. 100% of user requests take more than 1 second. 
  :
}

################################################################################
##                          Scenarios                                         ##
################################################################################
# Read Carefully:
#
# The following questions present you with various scenarios in which database
# choices need to be made. For each scenario, you are presented with data
# available to make the decision and presented with three database choices.
# Please select the most appropriate choice in each scenario. Following these
# are various reasons which justify this database choice. Please select ALL
# characteristics which apply to the selected database, regardless of whether
# they also apply to the other databases as well.
#
# For questions asking you to select a single choice, print the
# character corresponding to the choice. For e.g., If the database corresponding
# to choice "A" is appropriate:
# echo "A"
#
# For questions asking you to pick characteristics corresponding to these
# databases, more than one choice may be correct. Please print "ALL"
# correct choices as a SINGLE NUMBER, without spaces or commas. For e.g.
# If the characteristics 1 and 2 prove useful:
# echo 12
#
#
# SCENARIO 1:
# We are starting a new company, and have decided to use a database to store
# information for each user who registers on our social network website.
# The relationships among users are critical for the business, and the website
# must be able to reflect each user's social network in a timely manner.
#
# After considering our requirements, we design our database with the following
# attributes:
#
# ID
# First Name
# Last Name
# Gender
#
# After observing the website usage for a month, we decide to make some
# changes to our application. First, we will require that each new user
# provides their telephone number when they sign up to prevent our service
# from being spammed. However, we do not need this information from our
# current users (since we have already removed all spammer accounts and the
# information is not used anywhere else). To store the phone numbers, we
# modify our database design to include a new column. The modified schema now
# looks like:
#
# ID
# First Name
# Last Name
# Gender
# Phone Number
#
# A year later, the website is flooded with users and billions of users are
# registered on it. To improve the user experience we conduct a market
# analysis and find the users would be better served if we ask them for their
# zip codes and tailor our offering based on their location. To store this, we
# modify our database design to include a new column as such:
#
# ID
# First Name
# Last Name
# Gender
# Phone Number
# Address

q5() {
  # If you are the tech lead of our company and if you could have foreseen
  # these kind of changes from the very beginning, keeping in mind the advantages
  # and disadvantages of each of the three databases you have worked with in
  # this project, which data storage would you suggest that our company should use
  # and why?
  # (Single Choice)
  #
  # A. MySQL
  # B. Neo4j
  # C. MongoDB
  :
}

q6() {
  # Which of the characteristics would have led you to choose the database
  # you did in the previous question?
  # (Choose all that apply)
  #
  # 1. Rigidity of the structured data
  # 2. Data Integrity
  # 3. Dynamic Schema
  # 4. Native support for relationships
  # 5. Document Storage
  :
}

# SCENARIO 2:
# Consider a system which stores the inventory of a warehouse, for an online
# shopping portal. We need to select a database to store information about the
# products stored in it.

# The database will only need to store generic information about each product
# and we are sure that we will not need to store any additional information
# about each product in the future. Some of the information stored includes:
# 1. ID
# 2. Name
# 3. Categories
# 4. Dimensions
# 5. Color
# 6. Quantities
# 7. Location in the warehouse

# Our database needs to support the following questions/operations and it is
# unlikely we will need to include additional functionality in our database:
# 1. Add a certain product to the warehouse.
# 2. Check whether a certain product is present in the warehouse
# 3. Get the quantity of a certain product present in the warehouse
# 4. Removal of products from the warehouse
# 5. Get a list of all the products present in warehouse
# 6. Get a list of products present in a specific location/bay in the warehouse
# 7. Movements of products within the warehouse in case of maintenance

# Our online shopping portal uses this database to check if a product is
# available before the customer is allowed to buy the product. Thus the
# database should ensure the integrity of data in the system.

q7() {
  # Question:
  # If you are the tech lead of our company, what kind of database would you
  # suggest our company to use for managing each of these warehouses?
  # (Single Choice)
  #
  # A. MySQL
  # B. Neo4j
  # C. MongoDB
  :
}

q8() {
  # Question:
  # Which of the characteristics would have led you to choose the database
  # you did in the previous question?
  # (Choose all that apply)
  #
  # 1. Rigidity of the structured data
  # 2. Data Integrity
  # 3. Dynamic Schema
  # 4. Document Storage
  :
}

# SCENARIO 3:
# Your company is building an application which stores information on
# restaurants in Pittsburgh to provide dining recommendations to its users.
# The application takes into account various factors about the restaurants
# themselves including location, price level, cuisine, average customer rating,
# etc., which are added and removed based on user feedback and frequency of use.
#
# The application allows the users to filter for restaurants based on multiple
# criteria. For example, the user could ask for “Restaurants, having a price
# rating of $$, serving american cuisine, with an average customer rating
# above 3 and located in 15213”.

# The application itself does not require strict constraints on the integrity of
# the data but should be able to handle a large amount of load to serve a large
# number of users simultaneously.

# Also, your company is planning to add more functionality in terms of the data
# being stored about each of the restaurants. It plans to introduce restaurant
# menus, restaurant photos, among others, which would then be available to users
# when filtering restaurants and would be shown in the results.

q9() {
  # Question:
  # Based on the above requirements, which database would you suggest that the
  # company use?
  # (Single Choice)
  #
  # A. MySQL
  # B. Neo4j
  # C. MongoDB
  :
}

q10() {
  # Question:
  # Which of the characteristics would have led you to choose the database
  # you did in the previous question?
  # (Choose all that apply)
  #
  # 1. Rigidity of the structured data
  # 2. Data Integrity
  # 3. Dynamic Schema
  # 4. Document Storage
  # 5. Support for Indexing
  # 6. High Scalability
  :
}

###############################################################################
##                    DO NOT MODIFY ANYTHING BELOW                           ##
###############################################################################
declare -ar questions=( "q1" "q2" "q3" "q4" "q5" "q6" "q7" "q8" "q9" "q10")
last=${questions[$((${#questions[*]}-1))]}
readonly last
readonly usage="This program is used to execute your solution.
Usage:
./runner.sh to run all the questions
./runner.sh -r <question_id> to run one single question
Example:
./runner.sh -r q1 to run q1"

contains() {
  local e
  for e in "${@:2}"; do
  [[ "$e" == "$1" ]] && return 0;
  done
  return 1
}

run() {
  if contains "$1" "${questions[@]}"; then
  echo -n "$("$1")"
  fi
}

while getopts ":hr:" opt; do
  case $opt in
  h)
    echo "$usage" >&2
    exit
  ;;
  r)
    question=$OPTARG
    if contains "$question" "${questions[@]}"; then
    run "$question"
    else
    echo "Invalid question id" >&2
    echo "$usage" >&2
    exit 2
    fi
    exit
  ;;
  \?)
    echo "Invalid option: -$OPTARG" >&2
    echo "$usage" >&2
    exit 2
  ;;
  esac
done

if [ -z "$1" ]; then
  #echo "The answers generated by executing your solution are: " >&2
  echo "{"
  for question in "${questions[@]}"; do
  echo -n ' '\""$question"\":\""$(run "$question")"\"
  if [[ "${question}" == "$last" ]]; then
    echo ""
  else
    echo ","
  fi
  done
  echo "}"
else
  echo "Invalid usage" >&2
  echo "$usage" >&2
  exit 2
fi
