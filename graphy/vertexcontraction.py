import itertools
from pyspark.rdd import RDD


def vertex_contraction(links, vertices_to_remove):
    """
    Perform Vertex Contraction on a series of links data structure, based on a list of vertices to remove.
    :param links:
    :param vertices_to_remove:
    :return:
    """
    if isinstance(links, list):
        contraction_func = remove_single_item_list
    elif isinstance(links, RDD):
        contraction_func = remove_single_item_rdd
    else:
        raise NotImplementedError("The 'links' parameter should be of type list or RDD")

    links_cleaned = links
    for item in vertices_to_remove:
        links_cleaned = contraction_func(links_cleaned, item)
    return links_cleaned


def remove_single_item_list(links, vertex):
    SOURCE = 0
    LINK = 1
    TARGET = 2

    starting_from = filter(lambda x: x[TARGET] == vertex, links)
    ending_in = filter(lambda x: x[SOURCE] == vertex, links)

    links_to_add = map(lambda x:
                       {
                           LINK: '-'.join([x[0][LINK], x[1][LINK]]),
                           SOURCE: x[0][SOURCE],
                           TARGET: x[1][TARGET],
                           'added': True
                       }, itertools.product(starting_from, ending_in))

    return list(filter(lambda x: not (x[TARGET] == vertex or x[SOURCE] == vertex),
                       links)) + list(links_to_add)


def remove_single_item_rdd(links, vertex):
    SOURCE = 'source'
    LINK = 'link'
    TARGET = 'target'

    starting_from = links.filter(lambda x: x[TARGET] == vertex)
    ending_in = links.filter(lambda x: x[SOURCE] == vertex)

    links_to_add = (starting_from
                    .cartesian(ending_in)
                    .map(lambda x:
                         {
                             LINK: '-'.join([x[0][LINK], x[1][LINK]]),
                             SOURCE: x[0][SOURCE],
                             TARGET: x[1][TARGET],
                             'added': True
                         })
                    )

    return (links
            .filter(lambda x: not (x[TARGET] == vertex or x[SOURCE] == vertex))
            .union(links_to_add)
            )


if __name__ == '__main__':
    """
    Let's try a trivial example.
    we have the following links:
        T_ORIGINAL_DATA -> V_DATA -> T_DATA_WORK -> T_DATA_FINAL
        
    and we want to end up with:
        T_ORIGINAL_DATA -> T_DATA_FINAL.

    We have to remove the two vertices in the middle.
    """

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[*]").getOrCreate()

    vertices_list = [
        ('T_ORIGINAL_DATA',),
        ('V_DATA',),
        ('T_DATA_WORK',),
        ('T_DATA_FINAL',)
    ]

    links_list = [
        ('T_ORIGINAL_DATA', 'pt', 'V_DATA'),
        ('V_DATA', 'si', 'T_DATA_WORK'),
        ('T_DATA_WORK', 'pt', 'T_DATA_FINAL')
    ]

    vertices_df = spark.createDataFrame(vertices_list, ['vertex'])
    links_df = spark.createDataFrame(links_list, ['source', 'link', 'target'])

    vertices_df.show()
    links_df.show()

    # RDDs
    vertices_rdd = vertices_df.rdd.map(lambda x: x.asDict())
    links_rdd = links_df.rdd.map(lambda x: x.asDict())

    vertices_to_remove = (vertices_rdd
                          .map(lambda x: x['vertex'])
                          .filter(lambda x: x.startswith('V') or x.rfind('WORK') != -1)
                          .collect()
                          )
    vertices_cleaned_rdd = vertices_rdd.filter(lambda x: x['vertex'] not in vertices_to_remove)

    links_cleaned_rdd = vertex_contraction(links_rdd, vertices_to_remove)
    print("RDD -> Links cleaned:", links_cleaned_rdd.collect())

    # LISTs
    vertices_to_remove = filter(lambda x: x.startswith('V') or x.rfind('WORK') != -1,
                                map(lambda x: x[0], vertices_list))
    vertices_cleaned_list = filter(lambda x: x[0] not in vertices_to_remove, vertices_list)

    links_cleaned_list = vertex_contraction(links_list, vertices_to_remove)
    print("LIST -> Links cleaned:", links_cleaned_list)
