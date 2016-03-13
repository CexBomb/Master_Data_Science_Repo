# coding: utf-8

from pyspark import SparkContext

# In[1]:
sc = SparkContext(appName='outliers_kschool17')
data = sc.textFile('data/sales_segments.csv.gz')


# In[2]:

data.first()


# In[3]:

data.map(lambda l: l.split('^')[51]).take(3)


# In[ ]:

#eliminamos el header


# In[4]:

header = data.first()


# In[5]:

lines = data.filter(lambda line: line != header)


# In[6]:

lines.first()


# In[ ]:

#sacamos los campos que nos interesan


# In[13]:

fields = ['bookings_seg', 'revenue_amount_seg', 'fuel_surcharge_amount_seg']


# In[14]:

[header.split('^').index(field) for field in fields]


# In[15]:

def average_fare_tax(line):
    elems = line.split('^')
    bookings = float(elems[49])
    rev = float(elems[51])
    tax = float(elems[53])
    
    av_rev = rev / bookings
    av_tax = tax / bookings
    
    return (av_rev, av_tax)


# In[16]:

average_fare_tax(lines.first())


# In[17]:

simple = lines.map(average_fare_tax)


# In[19]:

simple.take(3)


# In[ ]:

#Kmeans clustering


# In[21]:

from pyspark.mllib.clustering import KMeans, KMeansModel


# In[22]:

clusters = KMeans.train(simple, 10)


# In[23]:

clusters.save(sc,'clusters')


# In[24]:

clusters.centers


# In[27]:

clusters.predict((33.56, 7.3))


# In[30]:

bycluster = simple.map(lambda point: (clusters.predict(point), point))


# In[31]:

bycluster.take(3)


# In[ ]:

#calcular media y sigma de cada punto


# In[32]:

bycluster.cache()


# In[ ]:

#means = bycluster.reduceByKey(calc_avg_reduce)


# In[33]:

bycluster_withones = simple.map(lambda (x,y): (clusters.predict((x,y)), (x,y,1)))


# In[34]:

bycluster_withones.take(3)


# In[39]:

bycluster_withones.cache()


# In[35]:

def calc_avg_reduce(acc, p):
    (rev_p, tax_p, c) = p
    (rev_ac, tax_ac, c_acc) = acc
    
    result = (rev_p + rev_ac, tax_p + tax_ac, c + c_acc)
    
    return result


# In[37]:

avs = bycluster_withones.reduceByKey(calc_avg_reduce)


# In[38]:

avs.take(3)


# In[ ]:

#hacemos un join para asignar a cada punto la media de su grupo.


# In[44]:

def medias(clust):
    rev_t, tax_t, count = clust
    return (rev_t / count, tax_t / count, count)


# In[45]:

avs.mapValues(medias).take(3)


# In[46]:

averages = avs.mapValues(medias)


# In[ ]:

#Esto esta mal


# In[73]:

#simple.join(averages).take(3)


# In[ ]:

#Asi mejor


# In[47]:

bycluster_withones.join(averages).take(3)


# In[48]:

def sq_diffs(p_with_cluster_mean):
    
    ((rev, tax, n),(av_rev, av_tax, n_cl)) = p_with_cluster_mean
    
    rev_sqdiff = (rev - av_rev) ** 2
    tax_sqdiff = (tax - av_tax) ** 2
    
    return (rev_sqdiff, tax_sqdiff, 1)


# In[49]:

points_w_means_sqdiffs = bycluster_withones.join(averages).mapValues(sq_diffs)


# In[50]:

points_w_means_sqdiffs.take(3)


# In[53]:

total_sqdiffs = points_w_means_sqdiffs.reduceByKey(calc_avg_reduce)


# In[54]:

total_sqdiffs.take(3)


# In[55]:

from math import sqrt
def stdevs(sqdiffs_percluster):
    (sqd_rev_total, sqd_tax_total, n_cl) = sqdiffs_percluster
    
    std_rev = sqrt(sqd_rev_total / n_cl)
    std_tax = sqrt(sqd_tax_total / n_cl)
    
    return (std_rev, std_tax)


# In[57]:

total_sqdiffs.mapValues(stdevs).take(11)


# In[58]:

stds_clusters = total_sqdiffs.mapValues(stdevs)


# In[59]:

cluster_stats = averages.join(stds_clusters)


# In[62]:

cluster_stats.take(3)


# In[65]:

full = bycluster.join(cluster_stats)


# In[66]:

full.take(3)


# In[75]:

def zscore(p_w_stats):
    
    (p, stats) = p_w_stats
    rev, tax = p
    means, stdevs = stats
    (rev_m, tax_m, _) = means
    (rev_std, tax_std) = stdevs
    
    
    if rev_std == 0:
        return rev, tax, 3000, True 
        
    zscore_rev = (rev - rev_m) / rev_std
    zscore_tax = (tax - rev_m) / tax_std
    
    zscore = sqrt(zscore_rev ** 2 + zscore_tax ** 2)
    outlier = zscore > 3
    
    return rev, tax, zscore, outlier


# In[70]:

zscore(full.first()[1])


# In[76]:

outliers = full.mapValues(zscore)


# In[72]:

outliers.take(3)


# In[77]:

outliers.saveAsTextFile('outliers')


# In[ ]:



