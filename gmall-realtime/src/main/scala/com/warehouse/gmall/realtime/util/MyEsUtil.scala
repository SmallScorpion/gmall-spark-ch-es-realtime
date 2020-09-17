package com.warehouse.gmall.realtime.util

import java.{lang, util}

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder



object MyEsUtil {

  // 连接池
  var factory: JestClientFactory = null

  // 获取jest
  def getClient:JestClient ={
    // 懒加载
    if(factory==null)build();
    factory.getObject

  }

  // 加载
  def  build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200" )
      // 是否多线程
      .multiThreaded(true)
      // 最大连接数(并发数)
      .maxTotalConnection(20)
      // 连接超时时间
      .connTimeout(10000).readTimeout(10000).build())
  }

  // 插入
  def addDoc(): Unit ={

    val jest: JestClient = getClient

    // Builder(可转化为json对象)
    val index: Index = new Index.Builder(Movie( "01", "tmp", "test" )).index("movie_test").`type`("_doc").id("01").build()

    val message: String = jest.execute( index ).getErrorMessage

    if(message != null){
      println(message)
    }


    jest.close()

  }

  def queryDoc(): Unit ={

    val jest: JestClient = getClient

    val str: String =
      """
        |{
        |  "query": {
        |    "bool": {
        |      "must": [
        |        {
        |          "match": {
        |            "name": "operation"
        |          }
        |        }
        |      ],
        |      "filter": {
        |        "term": {
        |          "actorList.name.keyword": "zhang han yu"
        |        }
        |      }
        |    }
        |  },
        |  "from": 0,
        |  "size": 20,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |
        |}
        |""".stripMargin
//1

    // 使用对象创建方式，适合增改查询结构
    val boolQueryBuilder = new BoolQueryBuilder()
      .must( new MatchQueryBuilder( "name", "operation" ) )
      .filter( new TermQueryBuilder( "actorList.name.keyword", "zhang han yu" ) )

    val searchSourceBuilder = new SearchSourceBuilder()
      .query(boolQueryBuilder)
      .from( 0).size(20)
      .sort( "doubanScore", SortOrder.DESC )
      .highlight( new HighlightBuilder().field( "name" ) )

    val query2: String = searchSourceBuilder.toString

    val search = new Search.Builder(str).addIndex("movie_index").addType("movie").build()

    val result: SearchResult = jest.execute( search )

    // 结果封装使用需要用util
    val hits: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits( classOf[ util.Map[String, Any] ] )
    import scala.collection.JavaConversions._
    for ( hit <- hits ){
      println(hit.source.mkString(","))
    }

    val list: List[lang.Double] = hits.map( _.score ).toList
    println(hits.mkString("\n"))
    jest.close()

  }


  /**
   * 批量保存
   */
  def bulkDoc( sourceList: List[Any], indexName: String ): Unit = {

    if ( sourceList != null && sourceList.nonEmpty ) {

      val jest: JestClient = getClient

      // 构造批次操作
      val bulkBuilder = new Bulk.Builder
      for (source <- sourceList ) {
      val index = new Index.Builder(source).index(indexName).`type`("_doc").build()
        bulkBuilder.addAction(index)
      }

      val result: BulkResult = jest.execute(bulkBuilder.build())
      val items: util.List[BulkResult#BulkResultItem] = result.getItems
      // println("保存到es: " + items.size() + " 条数")
      jest.close()
    }
  }


  def main(args: Array[String]): Unit = {
    queryDoc()
  }


  case class Movie ( id: String,  movie_name: String, name: String )

}
