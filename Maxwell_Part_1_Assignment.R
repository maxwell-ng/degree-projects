#PART ONE: R ASSGNMENT
#COURSE: AC51002
#BY: NDUGATUDA MAXWELL

# HOW THE USE OF BABY NAMES IN THE USA CHANGED OVER A PERIOD OF TIME


# NOTE: To run this code you will need to install tidyverse package into your machine if you haven't done that.
# IMPORT LIBRARIES
library('dplyr')
library('ggplot2')

#Import data
# You need to 'install.packages('babynames')' to use babynames dataset
library('babynames')

# Explore the data
head(babynames)
tail(babynames)
view(babynames)
summarise(babynames)

# use filter to extract names from 1980 to 2009
df1 <- babynames %>%
  filter(year<2010)

# use filter to extract names from 2010 to 2017
df2 <- babynames %>%
  filter(year>2009)

#Drops all observations in df2 that match in df1
# anti_join function will extract names used from 2010 to 2017 
# which were not used in the previous years

df3 <- anti_join(df2, df1, by="name")
df3

# Visualization: This summarize the usage of these names over the period eight years
count_names <- df3%>%
  group_by(year)%>%
  summarise(Total=n())
count_names

#BOXPLOT
ggplot(count_names, mapping = aes(year,Total))+
  geom_boxplot(stat = "boxplot", position = "dodge2")+
  ggtitle("Name Use Changes Over Time")

#BAR GRAPH
ggplot(data=count_names, mapping = aes(year,Total))+
  geom_bar(stat = "identity") +
  ggtitle("Name Use Changes Over Time") +
  theme(legend.position = "none")+
  scale_y_continuous(labels = comma)

