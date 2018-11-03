import itertools
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame

DEFAULT_LINKS_MERGER = '-'.join


def vertex_smoothing(links, vertices_to_remove, source_getter, target_getter,
                     link_getter, obj_creator, links_merger=DEFAULT_LINKS_MERGER):
    """
    Perform Vertex Smoothing on a series of links data structure, based on a list of vertices to remove.
    :param links:
    :param vertices_to_remove:
    :param source_getter:
    :param link_getter:
    :param target_getter:
    :param obj_creator:
    :param links_merger:
    :return:
    """
    if isinstance(links, list):
        smoothing_func = remove_single_item_list
    elif isinstance(links, RDD):
        smoothing_func = remove_single_item_rdd
    elif isinstance(links, DataFrame):
        smoothing_func = remove_single_item_df
    else:
        raise NotImplementedError("The 'links' parameter should be of type list or RDD")

    links_cleaned = links
    for item in vertices_to_remove:
        links_cleaned = smoothing_func(links_cleaned, item, source_getter,
                                       target_getter, link_getter, obj_creator, links_merger)
    return links_cleaned


def remove_single_item_list(links, vertex, source_getter, target_getter,
                            link_getter, obj_creator, links_merger):
    starting_from = filter(lambda x: target_getter(x) == vertex, links)
    ending_in = filter(lambda x: source_getter(x) == vertex, links)

    links_to_add = map(lambda x:
                       obj_creator(source_getter(x[0]),
                                   links_merger([link_getter(x[0]), link_getter(x[1])]),
                                   target_getter(x[1])),
                       itertools.product(starting_from, ending_in))

    return list(filter(lambda x: not (target_getter(x) == vertex or
                                      source_getter(x) == vertex),
                       links)) + list(links_to_add)


def remove_single_item_rdd(links, vertex, source_getter, target_getter,
                           link_getter, obj_creator, links_merger):
    starting_from = links.filter(lambda x: target_getter(x) == vertex)
    ending_in = links.filter(lambda x: source_getter(x) == vertex)

    links_to_add = (starting_from
                    .cartesian(ending_in)
                    .map(lambda x:
                         obj_creator(source_getter(x[0]),
                                     links_merger([link_getter(x[0]), link_getter(x[1])]),
                                     target_getter(x[1]))
                         )
                    )

    return (links
            .filter(lambda x: not (target_getter(x) == vertex or source_getter(x) == vertex))
            .union(links_to_add)
            )


def remove_single_item_df(links, vertex, source_getter, target_getter,
                          link_getter, obj_creator, links_merger):
    starting_from = links.filter(target_getter(links) == vertex)
    ending_in = links.filter(source_getter(links) == vertex)

    cross_join = (starting_from.toDF(*[c + '_left' for c in starting_from.columns])
                  .crossJoin(ending_in.toDF(*[c + '_right' for c in ending_in.columns]))
                  )

    links_to_add = (cross_join
                    .rdd
                    .map(lambda row:
                         obj_creator(source_getter(row, '_left'),
                                     links_merger([link_getter(row, '_left'), link_getter(row, '_right')]),
                                     target_getter(row, '_right')
                                     )
                         )
                    .toDF()
                    )

    return (links
            .filter(~((target_getter(links) == vertex) | (source_getter(links) == vertex)))
            .union(links_to_add.select(source_getter(links_to_add), link_getter(links_to_add),
                                       target_getter(links_to_add)))
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

    from graphy.smoothing.utils import (source_getter_dict, source_getter_tuple, link_getter_dict,
                                        link_getter_tuple, target_getter_dict, target_getter_tuple,
                                        obj_creator_dict, obj_creator_tuple)

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

    vertices_df = spark.createDataFrame(vertices_list, ['ITEM'])
    links_df = spark.createDataFrame(links_list, ['SOURCE', 'LINK', 'TARGET'])

    vertices_df.show()
    links_df.show()

    # RDDs
    vertices_rdd = vertices_df.rdd.map(lambda x: x.asDict())
    links_rdd = links_df.rdd.map(lambda x: x.asDict())

    vertices_to_remove = (vertices_rdd
                          .map(lambda x: x['ITEM'])
                          .filter(lambda x: x.startswith('V') or x.rfind('WORK') != -1)
                          .collect()
                          )
    vertices_cleaned_rdd = vertices_rdd.filter(lambda x: x['ITEM'] not in vertices_to_remove)

    links_cleaned_rdd = vertex_smoothing(
        links_rdd, vertices_to_remove, source_getter_dict, target_getter_dict,
        link_getter_dict, obj_creator_dict
    )
    print("RDD -> Links cleaned:", links_cleaned_rdd.collect())

    # LISTs
    vertices_to_remove = filter(lambda x: x.startswith('V') or x.rfind('WORK') != -1,
                                map(lambda x: x[0], vertices_list))
    vertices_cleaned_list = filter(lambda x: x[0] not in vertices_to_remove, vertices_list)

    links_cleaned_list = vertex_smoothing(
        links_list, vertices_to_remove, source_getter_tuple, target_getter_tuple,
        link_getter_tuple, obj_creator_tuple
    )
    print("LIST -> Links cleaned:", links_cleaned_list)

    """
    Let's try a trivial example.
    we have the following links:
         T_ORIGINAL_TABLE_1 -\                            /-> T_TABLE_FINAL_1 
                              -> V_TABLE -> T_TABLE_WORK -
         T_ORIGINAL_TABLE_2 -/                            \-> T_TABLE_FINAL_2
        
    and we want to end up with:
         T_ORIGINAL_TABLE_1 -\ /-> T_TABLE_FINAL_1 
                              X
         T_ORIGINAL_TABLE_2 -/ \-> T_TABLE_FINAL_2

    We have to remove the two vertices in the middle, and build more complex links.
    """
    vertices_list = [
        ("T_ORIGINAL_TABLE_1",),
        ("T_ORIGINAL_TABLE_2",),
        ("V_TABLE",),
        ("T_TABLE_WORK",),
        ("T_TABLE_FINAL_1",),
        ("T_TABLE_FINAL_2",)
    ]

    links_list = [
        ('T_ORIGINAL_TABLE_1', 'mi', 'V_TABLE'),
        ('T_ORIGINAL_TABLE_2', 'mi', 'V_TABLE'),
        ('V_TABLE', 'pt', 'T_TABLE_WORK'),
        ('T_TABLE_WORK', 'si', 'T_TABLE_FINAL_1'),
        ('T_TABLE_WORK', 'si', 'T_TABLE_FINAL_2')
    ]

    vertices_df = spark.createDataFrame(vertices_list, ["ITEM"])
    links_df = spark.createDataFrame(links_list, ["SOURCE", "LINK", "TARGET"])

    vertices_df.show()
    links_df.show()

    # # RDDs
    # vertices_rdd = vertices_df.rdd.map(lambda x: x.asDict())
    # links_rdd = links_df.rdd.map(lambda x: x.asDict())
    #
    # vertices_to_remove = (vertices_rdd
    #                       .map(lambda x: x['ITEM'])
    #                       .filter(lambda x: x.startswith('V') or x.rfind('WORK') != -1)
    #                       .collect()
    #                       )
    # vertices_cleaned_rdd = vertices_rdd.filter(lambda x: x['ITEM'] not in vertices_to_remove)
    #
    # links_cleaned_rdd = vertex_contraction(
    #     links_rdd, vertices_to_remove, source_getter_dict, target_getter_dict,
    #     link_getter_dict, obj_creator_dict
    # )
    # print("RDD -> Links cleaned:", links_cleaned_rdd.collect())

    # LISTs
    vertices_to_remove = filter(lambda x: x.startswith('V') or x.rfind('WORK') != -1,
                                map(lambda x: x[0], vertices_list))
    vertices_cleaned_list = filter(lambda x: x[0] not in vertices_to_remove, vertices_list)

    links_cleaned_list = vertex_smoothing(
        links_list, vertices_to_remove, source_getter_tuple, target_getter_tuple,
        link_getter_tuple, obj_creator_tuple
    )
    print("LIST -> Links cleaned:", links_cleaned_list)
