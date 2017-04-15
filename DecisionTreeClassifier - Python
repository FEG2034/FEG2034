import pandas
import numpy as np
import sklearn.tree
import matplotlib.pyplot as plt
import pydotplus 
import IPython

train = pandas.read_csv('C:/Users/user/Documents/train.csv', nrows=100)
test = pandas.read_csv('C:/Users/user/Documents/test.csv', nrows=100)
del train['visitor_hist_starrating']
del train['visitor_hist_adr_usd']
del test['visitor_hist_starrating']
del test['visitor_hist_adr_usd']

train['rate_percent_diff'] = np.matrix([[0.0] for i in range(len(train['srch_id']))])
test['rate_percent_diff'] = np.matrix([[0.0] for i in range(len(test['srch_id']))])
def rate_percent_diff(comp_rate, comp_rate_percent_diff):
    for i in range(len(comp_rate)):
        if comp_rate[i]==1 or comp_rate[i]==-1:
            comp_rate_percent_diff[i] = comp_rate_percent_diff[i]*comp_rate[i]
        else:
            comp_rate_percent_diff[i] = 0.0
    return comp_rate_percent_diff + train['rate_percent_diff']
train['rate_percent_diff'] = rate_percent_diff(train['comp1_rate'], train['comp1_rate_percent_diff'])
train['rate_percent_diff'] = rate_percent_diff(train['comp2_rate'], train['comp2_rate_percent_diff'])
train['rate_percent_diff'] = rate_percent_diff(train['comp3_rate'], train['comp3_rate_percent_diff'])
train['rate_percent_diff'] = rate_percent_diff(train['comp4_rate'], train['comp4_rate_percent_diff'])
train['rate_percent_diff'] = rate_percent_diff(train['comp5_rate'], train['comp5_rate_percent_diff'])
train['rate_percent_diff'] = rate_percent_diff(train['comp6_rate'], train['comp6_rate_percent_diff'])
train['rate_percent_diff'] = rate_percent_diff(train['comp7_rate'], train['comp7_rate_percent_diff'])
test['rate_percent_diff'] = rate_percent_diff(test['comp8_rate'], test['comp8_rate_percent_diff'])
test['rate_percent_diff'] = rate_percent_diff(test['comp1_rate'], test['comp1_rate_percent_diff'])
test['rate_percent_diff'] = rate_percent_diff(test['comp2_rate'], test['comp2_rate_percent_diff'])
test['rate_percent_diff'] = rate_percent_diff(test['comp3_rate'], test['comp3_rate_percent_diff'])
test['rate_percent_diff'] = rate_percent_diff(test['comp4_rate'], test['comp4_rate_percent_diff'])
test['rate_percent_diff'] = rate_percent_diff(test['comp5_rate'], test['comp5_rate_percent_diff'])
test['rate_percent_diff'] = rate_percent_diff(test['comp6_rate'], test['comp6_rate_percent_diff'])
test['rate_percent_diff'] = rate_percent_diff(test['comp7_rate'], test['comp7_rate_percent_diff'])
test['rate_percent_diff'] = rate_percent_diff(test['comp8_rate'], test['comp8_rate_percent_diff'])

def comp_inv(comp_inv):
    for i in range(len(comp_inv)):
        if comp_inv[i]==1:
            comp_inv[i] = 1
        else:
            comp_inv[i] = 0
    return comp_inv
train['comp1_inv'] = comp_inv(train['comp1_inv'])
train['comp2_inv'] = comp_inv(train['comp2_inv'])
train['comp3_inv'] = comp_inv(train['comp3_inv'])
train['comp4_inv'] = comp_inv(train['comp4_inv'])
train['comp5_inv'] = comp_inv(train['comp5_inv'])
train['comp6_inv'] = comp_inv(train['comp6_inv'])
train['comp7_inv'] = comp_inv(train['comp7_inv'])
train['comp8_inv'] = comp_inv(train['comp8_inv'])
test['comp1_inv'] = comp_inv(test['comp1_inv'])
test['comp2_inv'] = comp_inv(test['comp2_inv'])
test['comp3_inv'] = comp_inv(test['comp3_inv'])
test['comp4_inv'] = comp_inv(test['comp4_inv'])
test['comp5_inv'] = comp_inv(test['comp5_inv'])
test['comp6_inv'] = comp_inv(test['comp6_inv'])
test['comp7_inv'] = comp_inv(test['comp7_inv'])
test['comp8_inv'] = comp_inv(test['comp8_inv'])

del train['comp1_rate'], train['comp1_rate_percent_diff']
del train['comp2_rate'], train['comp2_rate_percent_diff']
del train['comp3_rate'], train['comp3_rate_percent_diff']
del train['comp4_rate'], train['comp4_rate_percent_diff']
del train['comp5_rate'], train['comp5_rate_percent_diff']
del train['comp6_rate'], train['comp6_rate_percent_diff']
del train['comp7_rate'], train['comp7_rate_percent_diff']
del train['comp8_rate'], train['comp8_rate_percent_diff']
del test['comp1_rate'], test['comp1_rate_percent_diff']
del test['comp2_rate'], test['comp2_rate_percent_diff']
del test['comp3_rate'], test['comp3_rate_percent_diff']
del test['comp4_rate'], test['comp4_rate_percent_diff']
del test['comp5_rate'], test['comp5_rate_percent_diff']
del test['comp6_rate'], test['comp6_rate_percent_diff']
del test['comp7_rate'], test['comp7_rate_percent_diff']
del test['comp8_rate'], test['comp8_rate_percent_diff']

y_train = np.matrix(train['booking_bool'])
x_train = np.matrix(train['rate_percent_diff'])
regr_2 = sklearn.tree.DecisionTreeClassifier(max_depth=2) #最大深度為2的決策樹
regr_5 = sklearn.tree.DecisionTreeClassifier(max_depth=5) #最大深度為5的決策樹
regr_2.fit(x_train, y_train)
regr_5.fit(x_train, y_train)

x_test = np.matrix(test['rate_percent_diff'])
y_predict2 = regr_2.predict(x_test)
y_predict5 = regr_5.predict(x_test)

dot_data = sklearn.tree.export_graphviz(regr_2, feature_names='rate_percent_diff', class_names='booking_bool', out_file=None)
graph = pydotplus.graph_from_dot_data(dot_data)
IPython.display.Image(graph.create_png())
