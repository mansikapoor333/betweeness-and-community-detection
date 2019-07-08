
# coding: utf-8

# In[1]:


from pyspark import SparkContext
from operator import itemgetter
import sys
import operator


# In[4]:
thresh = int(sys.argv[1])
input_file = str(sys.argv[2])
betweenness_file = str(sys.argv[3])
community_file = str(sys.argv[4])

sc = SparkContext()


# In[5]:



rdd = sc.textFile(input_file)


# In[6]:


header = rdd.first()
data = rdd.filter(lambda x: x!=header).map(lambda x: x.split(','))


# In[7]:


#data.take(3)


# In[8]:


#data.count()


# In[9]:


#users = data.map(lambda x: x[0]).distinct().collect()


# In[11]:


#businesses = data.map(lambda x: x[1]).distinct().collect()

baskets = data.map(lambda x: (x[0], {x[1]})).reduceByKey(lambda x,y: x.union(y))


user_business = baskets.collect()


# In[15]:


#user_business


# In[16]:


edges = set()
for i in user_business:
    for j in user_business:
        if i[0] != j[0]:
            if len(i[1].intersection(j[1])) >= thresh:
                #if (j,i) not in edges:
                edges.add((i[0],j[0]))
                #edges.add((j,i))

M = len(edges)/2


'''for i in edges:
    if (i[1],i[0]) not in edges:
        print(i)
        print(False)'''
# In[21]:


A = {}
edges2 = {}
for edge in edges:
    i = edge[0]
    j = edge[1]
    if i not in A:
        A[i] = [j]
        edges2[i] = [j]
    else:
        A[i].append(j)
        edges2[i].append(j)
    '''if j not in A:
        A[j] = [i]
        edges2[j] = [i]
    else:
        A[j].append(i)
        edges2[j].append(i)'''


'''s = 0
for k,v in A.items():
    s += len(v)
s/2'''

# In[32]:


def get_betweenness(edges2):

    #initialization

    bet_list = []
    for k,v in edges2.items():
        for n in v:
            bet_list.append(tuple([(k,n),-1]))

    bet_values = {}
    for i in bet_list:
        bet_values[i[0]] = i[1]


    for root in edges2:
        queue = []
        stack = []
        d_root = dict()
        parent = dict()
        paths = dict()

        for node in edges2:
            if node==root:
                continue
            d_root[node] = float('inf')
            paths[node] = 0
            parent[node] = []
        d_root[root] = 0
        paths[root] = 1
        parent[node] = []
        queue.append(root)

        while len(queue)>0:
            v = queue.pop(0)
            stack.append(v)
            for w in edges2[v]:
                if d_root[w] != float('inf') and d_root[w] != d_root[v] + 1:
                    continue
                else:
                    if d_root[w] == float('inf'):
                        d_root[w] = d_root[v] + 1
                        queue.append(w)

                    if d_root[w] == d_root[v] + 1:
                        paths[w] = paths[w] + paths[v]
                        parent[w].append(v)

        depends = dict()
        for node in edges2:
            depends[node] = 0

        while len(stack)>0:
            w = stack.pop()
            if w != root:
                for v in parent[w]:

                    c = (paths[v]/paths[w])*(1 + depends[w])
                    bet_values[(v,w)] = bet_values[(v, w)] + c
                    depends[v] = depends[v] + c

    for each in bet_values:
        bet_values[each] += 1

    return bet_values

# In[33]:


betweenness = get_betweenness(edges2)

bet_out = {}
for each in betweenness:
    sorted_key = tuple(sorted(each))
    bet_out[sorted_key] = betweenness[each]

bet_out = sorted(bet_out.items(), key=operator.itemgetter(0))
bet_out = sorted(bet_out, key = itemgetter(1), reverse=True)
with open(betweenness_file, 'w') as fp:
    for i in bet_out:
        fp.write(str(i)[1:-1])
        fp.write('\n')

#len(betweenness)

# In[45]:

dicK = {}
for i in edges2:
    dicK[i] = len(edges2[i])


# In[48]:


def get_communities(e2):
    edge_graph = {}



    for each in e2.keys():
        edge_graph[each] = []
        for i in e2[each]:
            edge_graph[each].append((each,i))

    new_graph = {node: set(each for edge in e for each in edge)
                 for node, e in edge_graph.items()}

    def connected_components(neighbors):
        visited = set()
        def component(node):
            nodes = set([node])
            while nodes:
                node = nodes.pop()
                visited.add(node)
                nodes = nodes | neighbors[node] - visited
                yield node
        for node in neighbors:
            if node not in visited:
                yield component(node)

    components = []
    for component in connected_components(new_graph):
        c = set(component)
        x = []
        for e in edge_graph.values():
            for edge in e:
                if c.intersection(edge):
                    x.append(edge)

        components.append(x)

    final_components = []
    for i in components:
        comp = set()
        for j in i:
            for k in j:
                comp.add(k)
        final_components.append(list(comp))

    return(final_components)


# In[49]:


final_components = get_communities(edges2)


#len(final_components)


# In[53]:


def calc_modularity(communities):
    mod = 0

    s = 0
    while s < len(communities):
        i = 0
        while i < len(communities[s]):
            j = 0
            while j < len(communities[s]):
                a = int(communities[s][i] in A[communities[s][j]] or communities[s][j] in A[communities[s][i]])
                ki = dicK[communities[s][i]]
                kj = dicK[communities[s][j]]
                numerator = ki*kj
                denominator = 2*M
                addition = a - (float(numerator) / denominator)
                mod += addition

                j+= 1
            i+= 1
        s+=1


    mod /= 2*M
    return(mod)

modularity = calc_modularity(final_components)

# In[57]:


#from time import time


# In[58]:


max_mod = -1
#modularity = -1
previous_components = []
#start = time()
while modularity >= max_mod:
    betweenness = get_betweenness(edges2)
    max_bet = max(betweenness.items(), key = itemgetter(1))[1]#[1]
    del_edges = []
    for i in betweenness:
        if round(betweenness[i]) == round(max_bet):
            del_edges.append((i[0],i[1]))
            del_edges.append((i[1],i[0]))

    for i in del_edges:
        if i in edges:
            edges.remove(i)

    edges2 = {}
    for edge in edges:
        i = edge[0]
        j = edge[1]
        if i not in edges2:
            edges2[i] = [j]
        else:
            edges2[i].append(j)
        if j not in edges2:
            edges2[j] = [i]
        else:
            edges2[j].append(i)

    '''dicK = {}
    for i in edges2:
        dicK[i] = len(edges2[i])'''

    final_components = get_communities(edges2)

    modularity = calc_modularity(final_components)

    print("modularity", modularity)
    print("max_mod", max_mod)

    if modularity >= max_mod:
        max_mod = modularity
        previous_components = final_components


    else:
        break


#len(final_components)

# In[62]:


#print(max_mod)


# In[63]:


#print(len(previous_components))

previous_components.sort(key = len)
candidates = {x:[] for x in range(1, len(previous_components[-1])+1)}
for x in previous_components:
    candidates[len(x)].append(tuple(sorted(x)))

with open(community_file, 'w') as fp:
    #fp.write('Candidates:\n')
    for k in candidates:
        if len(candidates[k]) < 1:
            continue
        candidates[k] = list(set(candidates[k]))
        candidates[k].sort()
        s = str(candidates[k]).replace(',)', ')').replace('(','').replace(')', '\n').replace('\n, ', '\n')
        fp.write(s[1:-1])
        #fp.write('\n')
# In[64]:


#previous_components


# In[65]:


#del_edges
