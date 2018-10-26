from pyspark.sql import Row


# Utils functions ============================
# TUPLE


def source_getter_tuple(x):
    return x[0]


def source_setter_tuple(tup, s):
    new_tup = (s,) + tup[1:]
    return new_tup


def target_getter_tuple(x):
    return x[2]


def target_setter_tuple(tup, t):
    new_tup = tup[:2] + (t,) + tup[3:]
    return new_tup


def link_getter_tuple(x):
    return x[1]


def link_setter_tuple(tup, l):
    new_tup = tup[:1] + (l,) + tup[2:]
    return new_tup


def obj_creator_tuple(s, l, t):
    return (s, l, t)


# DICT
def source_getter_dict(x):
    return x['SOURCE']


def source_setter_dict(diz, s):
    new_diz = diz.copy()
    new_diz['SOURCE'] = s
    return new_diz


def target_getter_dict(x):
    return x['TARGET']


def target_setter_dict(diz, t):
    new_diz = diz.copy()
    new_diz['TARGET'] = t
    return new_diz


def link_getter_dict(x):
    return x['LINK']


def link_setter_dict(diz, l):
    new_diz = diz.copy()
    new_diz['LINK'] = l
    return new_diz


def obj_creator_dict(s, l, t):
    return {'SOURCE': s, 'LINK': l, 'TARGET': t}


# ROW
def target_getter_df(df, suffix=''):
    return df['TARGET' + suffix]


def source_getter_df(df, suffix=''):
    return df['SOURCE' + suffix]


def link_getter_df(df, suffix=''):
    return df['LINK' + suffix]


def obj_creator_df(source, link, target):
    return Row(**{'SOURCE': source, 'LINK': link, 'TARGET': target})
