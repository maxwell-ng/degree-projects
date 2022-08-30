% AC50002 - Programming Language for Data Engineering
% MATLAB Assignment
% By: Maxwell Ndugatuda
% ID No: 2477969

% Question 1: 

a = 0; %mean
b = 1; %standard deviation
N = 200; %range of real numbers to be generated

rng 'default'; %for reproducibility
initial_data = b*randn(N,1) + a; % generate normal distributed numbers

% plot histogram
hist1 = histogram(initial_data,BinMethod="fd");
% BinMethod="fd" determines the number of bins in the histogram
% based on Freedman-Diaconis rule
xlabel('range');
ylabel('count');
title('Normal Distribution of 200 Numbers');
savefig('histogram_of_200_random_numbers.fig');
close gcf


% Question 2:

%test null hypothesis to determine if the sample data is a normal distribution
null_hyp005 = chi2gof(initial_data); % 5% significance level

%The test above returned 0, which indicates that chi2gof accepted the
% null hypothesis at the default 5% significance level. 

%test null hypothesis at 1% significance level
null_hyp001 = chi2gof(initial_data,'Alpha',0.01);

%The test returned 0, which indicates that chi2gof accepted the
% null hypothesis at 1% significance level.

%To conclude, Yes the returned values of the hypothesis test indicates that the
%sample data is a Gaussian distribution

%Question 3:

num_to_Keep = 190; %numbers to keep 
remove_num0 = initial_data(randperm(numel(initial_data),num_to_Keep));
%Remove 10 numbers at random from the initial_data set

j = 0; %mean
s = 3; %standard deviation
uniform_num0 = s*rand(10,1) + j; %generates uniform distributed numbers

%Replace 10 numbers removed by concatenating the two Arrays
mixed_data0 = cat(1,remove_num0,uniform_num0);

%Question 4

%test null hypothesis of the mixed sample data in question 3 at 5% significance level
null_hyp3 = chi2gof(mixed_data0);
%The returned value null_hyp3 = 0 indicates that chi2gof does not reject the 
% null hypothesis at the default 5% significance level.

%test null hypothesis of sample data in question 3 at 1% significance level
null_hyp4 = chi2gof(mixed_data0, 'Alpha',0.01);
%The returned value null_hyp4 = 0 indicates that chi2gof does not reject the
% null hypothesis at the 1% significance level.

%Question 5

% (i) ============================================================= 

remove_num1 = mixed_data0(randperm(numel(mixed_data0),num_to_Keep));%remove 10 numbers

uniform_num1 = s*rand(10,1) + j; %generate random uniform numbers

mixed_data1 = cat(1,remove_num1,uniform_num1); %join two arrays vertically

null_hyp5 = chi2gof(mixed_data1); %test null hypothesis at 5% signifance level

null_hyp6 = chi2gof(mixed_data1, 'Alpha',0.01); %null hypothesis at 5% significance level

% (ii) =============================================================

remove_num2 = mixed_data1(randperm(numel(mixed_data1),num_to_Keep));

uniform_num2 = s*rand(10,1) + j; 

mixed_data2 = cat(1,remove_num2,uniform_num2); 

null_hyp7 = chi2gof(mixed_data2);
null_hyp8 = chi2gof(mixed_data2, 'Alpha',0.01); 

% (iii) =============================================================

remove_num3 = mixed_data2(randperm(numel(mixed_data2),num_to_Keep));

uniform_num3 = s*rand(10,1) + j; 

mixed_data3 = cat(1,remove_num3,uniform_num3);

null_hyp9 = chi2gof(mixed_data3); 
null_hyp10 = chi2gof(mixed_data3, 'Alpha',0.01); 

% (iv) =============================================================

remove_num4 = mixed_data3(randperm(numel(mixed_data3),num_to_Keep));

uniform_num4 = s*rand(10,1) + j; 

mixed_data4 = cat(1,remove_num4,uniform_num4);

null_hyp11 = chi2gof(mixed_data4); 
null_hyp12 = chi2gof(mixed_data4, 'Alpha',0.01); 

% (v) =============================================================

remove_num5 = mixed_data4(randperm(numel(mixed_data4),num_to_Keep));

uniform_num5 = s*rand(10,1) + j;

mixed_data5 = cat(1,remove_num5,uniform_num5); 

null_hyp13 = chi2gof(mixed_data5); 
null_hyp14 = chi2gof(mixed_data5, 'Alpha',0.01); 

% (vi) =============================================================

remove_num6 = mixed_data5(randperm(numel(mixed_data5),num_to_Keep));

uniform_num6 = s*rand(10,1) + j; 

mixed_data6 = cat(1,remove_num6,uniform_num6);

null_hyp15 = chi2gof(mixed_data6);
null_hyp16 = chi2gof(mixed_data6, 'Alpha',0.01); 

% (vii) =============================================================

remove_num7 = mixed_data6(randperm(numel(mixed_data6),num_to_Keep));

uniform_num7 = s*rand(10,1) + j; 

mixed_data7 = cat(1,remove_num7,uniform_num7); 

null_hyp17 = chi2gof(mixed_data7); 
null_hyp18 = chi2gof(mixed_data7, 'Alpha',0.01); 

% (viii) =============================================================

remove_num8 = mixed_data7(randperm(numel(mixed_data7),num_to_Keep));

uniform_num8 = s*rand(10,1) + j; 

mixed_data8 = cat(1,remove_num8,uniform_num8); 

null_hyp19 = chi2gof(mixed_data8);
null_hyp20 = chi2gof(mixed_data8, 'Alpha',0.01); 

% (ix) =============================================================

remove_num9 = mixed_data8(randperm(numel(mixed_data8),num_to_Keep));

uniform_num9 = s*rand(10,1) + j; 

mixed_data9 = cat(1,remove_num9,uniform_num9); 

null_hyp21 = chi2gof(mixed_data9); 
null_hyp22 = chi2gof(mixed_data9, 'Alpha',0.01); 

% (x) =============================================================

remove_num10 = mixed_data9(randperm(numel(mixed_data9),num_to_Keep));

uniform_num10 = s*rand(10,1) + j; 

mixed_data10 = cat(1,remove_num10,uniform_num10); 

null_hyp23 = chi2gof(mixed_data10); 
null_hyp24 = chi2gof(mixed_data10, 'Alpha',0.01); 

%Visualization

% in order to visualize the results of chi2gof in question 4 and 5
% concatenate results of all null hypothesis tested at 5% significance level
all_hyp005 = cat(1,null_hyp3,null_hyp5,null_hyp7, ...
    null_hyp9,null_hyp11,null_hyp13,null_hyp15, ...
    null_hyp17,null_hyp19,null_hyp21,null_hyp23);

% concatenate results of all null hypothesis tested at 1% significance level
all_hyp001 = cat(1,null_hyp4,null_hyp6,null_hyp8,null_hyp10, ...
    null_hyp12,null_hyp14,null_hyp16,null_hyp18,null_hyp20, ...
    null_hyp22,null_hyp24);

% plot histogram to show number of accepted and rejected null hypothesis at
% 0.05 significance level
C1 = categorical(all_hyp005,[0 1],{'yes','no'});
hist2 = histogram(C1);
ylabel('count');
title('Null Hypothesis at 5% Significance Level');
savefig('null_hypothesis_at_005_significance_level.fig');
close gcf;

% plot histogram to show number of accepted and rejected null hypothesis at
% 0.01 significance level
C2 = categorical(all_hyp001,[0 1],{'yes','no'});
hist3 = histogram(C2);
ylabel('count');
title('Null Hypothesis at 1% Significance Level');
savefig('null_hypothesis_at_001_significance_level.fig');
close gcf;
