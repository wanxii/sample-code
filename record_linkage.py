'''
Linking restaurant records in Zagat and Fodor's list using restaurant
names, cities, and street addresses.

Wanxi Zhou

'''
import csv
import jellyfish
import pandas as pd

import util


def find_matches(output_filename, mu, lambda_, block_on_city=False):
    '''
    Put it all together: read the data and apply the record linkage
    algorithm to classify the potential matches.

    Inputs:
      output_filename(string): the name of the output file
      mu(float) : the maximum false positive rate
      lambda_(float): the maximum false negative rate
      block_on_city(boolean): indicates whether to block on the city or not
    '''
    zagat_filename = "data/zagat.csv"
    fodors_filename = "data/fodors.csv"
    known_links_filename = "data/known_links.csv"
    unknown_links_filename = "data/unmatch_pairs.csv"

    zagat = import_database(zagat_filename)
    fodors = import_database(fodors_filename)
    match = pd.read_csv(known_links_filename, header = None)
    unmatch = pd.read_csv(unknown_links_filename, header = None)
    
    label = generate_label_dic(match, unmatch, zagat, fodors, mu, lambda_)

    try:
        with open(output_filename, "w", newline='') as f:
            writer = csv.writer(f)
            write_row(writer, label, block_on_city, zagat, fodors)

    except IOError:
        print("Could not write to file: {}".format(output_filename))
        return None


def similarity_tuple(index1, index2, rest_db1, rest_db2):
    '''
    Construct the similarity tuple of two restaurants.

    Inputs:
      index1, index2(integar): indicies of two restaurants
      rest_db1, rest_db2(dataframe): two restaurant databases

    Outputs:
      a similarity tuple comparing three fields: names,
      cities, and addresses with the form of
      (a, b, c), where a, b, c ∈ {"high", "medium", "low"}
    '''
    name1, city1, address1 = rest_db1.iloc[index1]
    name2, city2, address2 = rest_db2.iloc[index2]

    jw_name = jellyfish.jaro_winkler_similarity(name1, name2)
    jw_city = jellyfish.jaro_winkler_similarity(city1, city2)
    jw_address = jellyfish.jaro_winkler_similarity(address1, address2)

    return tuple(util.get_jw_category(i) for i in [jw_name, jw_city, jw_address])


def compute_sim_tp_prop(tr_db, lst, rest_db1, rest_db2):
    '''
    Compute the probabilities of all similarity tuples
    appearing in the given database.

    Inputs:
      tr_db(dataframe): database for training
      lst(list): list of all similarity tuples
      rest_db1, rest_db2(dataframe): two restaurant databases
 
    Outputs:
      a dictionary mapping similarity tuples to probabilities
    '''
    n = len(tr_db)
    d = {k: 0 for k in lst}

    for i in range(n):
        index1, index2 = tr_db.iloc[i]
        tp = similarity_tuple(index1, index2, rest_db1, rest_db2)
        d[tp] = (d[tp] * n + 1) / n

    return d


def gen_all_sim_tp(lst):
    '''
    Generate all possible similarity tuples.

    Inputs:
      lst(list): list of all similarity score categories

    Outputs:
      a list of all similarity tuples
    '''
    return [(i, j, k) for i in lst for j in lst for k in lst]


def label_match_unmatch(lst, rate, label, dic):
    '''
    A helper function for label_sim_tp.
    Classify similarity tuples as either "match" or "unmatch".

    Inputs:
      lst(list): list of similarity tuples
      rate(float): either maximum false positive rate
        or false negative rate
      label(string): either "match" or "unmatch"
      dic(dictionary): dictionary mapping similarity tuples to
        labels

    Outputs:
      rv(list): list of unlabeled similarity tuples
      dic(dictionary): updated input dictionary
    '''
    r, count = 0, 0
    for tp in lst:
        t, m, u = tp
        r += u if label == "match" else m
        if r <= rate:
            dic[t] = label
            count += 1
        else:
            break
    rv = lst[count:] if count < len(lst) else []

    return rv, dic


def label_sim_tp(sim_tp_lst, match_prop, unmatch_prop, fpr_ub, fnr_ub):
    '''
    Classify similarity tuples as "match", "unmatch" or
    "possible match".

    Inputs:
      sim_tp_lst(list): list of all similarity tuples
      match_prop(dictionary): dictionary mapping similarity
        tuples to probabilities yielded from the match database
      unmatch_prop(dictionary): dictionary mapping similarity
        tuples to probabilities yield from the unmatch database
      fpr_ub(float): maximum false positive rate
      fnr_ub(float): maxium false negative rate

    Outputs:
      a dictionary mapping all possible similarity tuples to labels
    '''
    rv = {}
    lst = []

    for tp in sim_tp_lst:
        m, u = match_prop[tp], unmatch_prop[tp]
        if (m == 0) and (u == 0):
            rv[tp] = "possible match"
        else:
            lst.append((tp, m, u))

    if len(lst) > 0:
        sorted_lst = util.sort_prob_tuples(lst)
        updated_lst, rv = label_match_unmatch(sorted_lst,
                                              fpr_ub, "match", rv)
        if len(updated_lst) > 0:
            final_lst, rv = label_match_unmatch(updated_lst[::-1],
                                                fnr_ub, "unmatch", rv)
            if len(final_lst) > 0:
                for tp in final_lst:
                    rv[tp[0]] = "possible match"
        
    return rv


def generate_label_dic(tr_db1, tr_db2, rest_db1, rest_db2, mu, lambda_):
    '''
    Combine all the functions and generate a dictionary
    mapping all possible similarity tuples to matching
    labels trained on given databases.

    Inputs:
      tr_db1, tr_db2(dataframe): two training databases
      rest_db1, rest_db2(dataframe): two restaurant databases
      mu(float) : the maximum false positive rate
      lambda_(float): the maximum false negative rate

    Outputs:
      a dictionary mapping similarity tuples to matching labels
    '''
    lst = gen_all_sim_tp(["high", "medium", "low"])
    m_prop = compute_sim_tp_prop(tr_db1, lst, rest_db1, rest_db2)
    u_prop = compute_sim_tp_prop(tr_db2, lst, rest_db1, rest_db2)

    return label_sim_tp(lst, m_prop, u_prop, mu, lambda_)


def import_database(filename):
    '''
    Import csv file as pandas dataframe.

    Inputs:
      filename (string): the name of the input file
    '''
    try:
        return (pd.read_csv(filename)
                  .set_index("index")
                  .astype(str))
    except IOError:
        print("Could not read from file: {}".format(filename))
        return None


def write_row(writer, label, block_on_city, rest_db1, rest_db2):
    '''
    Record restaurant pairs and matching results in rows
    of given csv file.

    Input:
      writer: writer of the given csv file
      label(dictionary): dictionary mapping similarity tuples
        to labels
      block_on_city(boolean): indicates whether to block on the city or not
      rest_db1, rest_db2(dataframe): two restaurant databases
    '''
    for i in range(len(rest_db1)):
        for j in range(len(rest_db2)):
            if block_on_city:
                if rest_db1.iloc[i, 1] != rest_db2.iloc[j, 1]:
                    continue
            t = similarity_tuple(i, j, rest_db1, rest_db2)
            writer.writerow([i, j, label[t]])
