# PART TWO: AC51002 - R ASSIGNMENT
# BY: NDUGATUDA MAXWELL

# EXPLORATORY DATA ANALYSIS USING UBER_2016 DATASET

#Import libraries/ dependencies
library(dplyr) #data manipulation/wrangling
library(ggplot2) #graph visualization
library(lubridate) #manipulate date and time
library(tidyr) #tidy data frame
library(readr) #read csv files
library(ggthemes) #add-on with ggplot
library(scales) #graphical scaling


#Load data Uber_2016.csv
data <- read_csv('https://raw.githubusercontent.com/maxtechsavvy/datasets/main/Uber_2016.csv')

#Explore the data
head(data) #view first six rows of data
tail(data) #view last six rows of data
summarise(data) #tibble
glimpse(data) #view data in a wide format
str(data) #display internal structure of data


#convert string into date and time using lubridate package
data$Start_Date_Time <- as_datetime(mdy_hm(data$`START_DATE*`))
data$End_Date_Time <- as_datetime(mdy_hm(data$`END_DATE*`))

#calculate during of each trips in minutes
data$trip_duration_min <- difftime(data$End_Date_Time, data$Start_Date_Time, units = "mins")
data$trip_duration_min <- as.numeric(data$trip_duration_min)


#statistics summary
summary(data) #mean, median, min, max, quartiles of the whole date


#PLOTING GRAPHS


#remove NA values
category <- data.frame(trip_category = na.omit(data$`CATEGORY*`))
category

#plot number of each trip category
#figure 1
trip_category <- ggplot(category, mapping = aes(x=trip_category, fill=trip_category))+
  geom_bar()
trip_category + ggtitle("Category of Trip")

#CALENDAR MONTH DAYS TRIPS

#plot number of trips per day
data$daytrip <- format(as.Date(data$Start_Date_Time,format="%Y-%m-%d"), format = "%d") #extract daily trips

#grouping
day_trip <- data%>%
  group_by(daytrip)%>%
  summarise(Total=n())
day_trip <- na.omit(day_trip)

#plotting
#figure 2
day_plot <- ggplot(day_trip, mapping = aes(daytrip,Total))+
  geom_bar(stat = "identity") +
  ggtitle("Number of Trips of Day in Calender Month") +
  theme(legend.position = "none")+
  scale_y_continuous(labels = comma)
day_plot

#MONTHLY TRIPS

#plot number of trips per month
data$monthly_trip <- format(as.Date(data$Start_Date_Time,format="%Y-%m-%d"), format = "%m") #extract monthly trips

#Grouping
month_trips <- data%>%
  group_by(monthly_trip)%>%
  summarise(Total=n())
month_trips <- na.omit(month_trips)

#plotting
#figure 3
month_plot <- ggplot(month_trips, mapping = aes(monthly_trip,Total, fill=monthly_trip))+
  geom_bar(stat = "identity") +
  ggtitle("Number of Trips per Month") +
  theme(legend.position = "none")+
  scale_y_continuous(labels = comma)
month_plot

#PURPOSE OF TRIPS

#number of trips for each purpose
purpose_trips <- data%>%
  group_by(`PURPOSE*`)%>%
  summarise(Total=n())
purpose_trips <- na.omit(purpose_trips)

#Ploting
#figure 4
purpose_plot <- ggplot(purpose_trips, mapping = aes(`PURPOSE*`,Total))+
  geom_bar(stat = "identity") +
  ggtitle("Number of Trips per Purpose") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))+
  scale_y_continuous(labels = comma)
purpose_plot
