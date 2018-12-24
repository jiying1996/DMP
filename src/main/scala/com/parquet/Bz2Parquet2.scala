package com.parquet

import com.utils.NBF
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Bz2Parquet2 {

  def main(args: Array[String]): Unit = {
    // 模拟企业开发模式，首先判断一下目录 是否为空
    if (args.length != 2) {
      println("目录不正确，退出程序！")
      sys.exit()
    }
    // 创建一个集合，存储输入输出目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local")
      // 处理数据，采取scala的序列化方式，性能比Java默认的高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    // 我们要采取snappy压缩方式，因为咱们现在用的是1.6版本的spark，到2.0以后就是默认的，不需要配置
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    val lines = sc.textFile("F:\\千锋学习\\8.Spark项目\\DMP\\2016-10-01_06_p1_invalid.1475274123982.log.FINISH.bz2")
    val dmpRDD = lines.map(t => t.split(",", t.length))
      .filter(_.length >= 85).map(
      arr => new DMP(
        arr(0),
        NBF.toInt(arr(1)),
        NBF.toInt(arr(2)),
        NBF.toInt(arr(3)),
        NBF.toInt(arr(4)),
        arr(5),
        arr(6),
        NBF.toInt(arr(7)),
        NBF.toInt(arr(8)),
        NBF.toDouble(arr(9)),
        NBF.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        NBF.toInt(arr(17)),
        arr(18),
        arr(19),
        NBF.toInt(arr(20)),
        NBF.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        NBF.toInt(arr(26)),
        arr(27),
        NBF.toInt(arr(28)),
        arr(29),
        NBF.toInt(arr(30)),
        NBF.toInt(arr(31)),
        NBF.toInt(arr(32)),
        arr(33),
        NBF.toInt(arr(34)),
        NBF.toInt(arr(35)),
        NBF.toInt(arr(36)),
        arr(37),
        NBF.toInt(arr(38)),
        NBF.toInt(arr(39)),
        NBF.toDouble(arr(40)),
        NBF.toDouble(arr(41)),
        NBF.toInt(arr(42)),
        arr(43),
        NBF.toDouble(arr(44)),
        NBF.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        NBF.toInt(arr(57)),
        NBF.toDouble(arr(58)),
        NBF.toInt(arr(59)),
        NBF.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        NBF.toInt(arr(73)),
        NBF.toDouble(arr(74)),
        NBF.toDouble(arr(75)),
        NBF.toDouble(arr(76)),
        NBF.toDouble(arr(77)),
        NBF.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        NBF.toInt(arr(84))
      )
    )

    val df = sqlContext.createDataFrame(dmpRDD)
    df.write.parquet(outputPath)
    sc.stop()
  }
}

class DMP(sessionid: String,
          advertisersid: Int,
          adorderid: Int,
          adcreativeid: Int,
          adplatformproviderid: Int,
          sdkversion: String,
          adplatformkey: String,
          putinmodeltype: Int,
          requestmode: Int,
          adprice: Double,
          adppprice: Double,
          requestdate: String,
          ip: String,
          appid: String,
          appname: String,
          uuid: String,
          device: String,
          client: Int,
          osversion: String,
          density: String,
          pw: Int,
          ph: Int,
          long: String,
          lat: String,
          provincename: String,
          cityname: String,
          ispid: Int,
          ispname: String,
          networkmannerid: Int,
          networkmannername: String,
          iseffective: Int,
          isbilling: Int,
          adspacetype: Int,
          adspacetypename: String,
          devicetype: Int,
          processnode: Int,
          apptype: Int,
          district: String,
          paymode: Int,
          isbid: Int,
          bidprice: Double,
          winprice: Double,
          iswin: Int,
          cur: String,
          rate: Double,
          cnywinprice: Double,
          imei: String,
          mac: String,
          idfa: String,
          openudid: String,
          androidid: String,
          rtbprovince: String,
          rtbcity: String,
          rtbdistrict: String,
          rtbstreet: String,
          storeurl: String,
          realip: String,
          isqualityapp: Int,
          bidfloor: Double,
          aw: Int,
          ah: Int,
          imeimd5: String,
          macmd5: String,
          idfamd5: String,
          openudidmd5: String,
          androididmd5: String,
          imeisha1: String,
          macsha1: String,
          idfasha1: String,
          openudidsha1: String,
          androididsha1: String,
          uuidunknow: String,
          userid: String,
          iptype: Int,
          initbidprice: Double,
          adpayment: Double,
          agentrate: Double,
          lomarkrate: Double,
          adxrate: Double,
          title: String,
          keywords: String,
          tagid: String,
          callbackdate: String,
          channelid: String,
          mediatype: Int
         ) extends Product {
  override def productElement(n: Int): Any = n match {
    case 0 => sessionid
    case 1 => advertisersid
    case 2 => adorderid
    case 3 => adcreativeid
    case 4 => adplatformproviderid
    case 5 => sdkversion
    case 6 => adplatformkey
    case 7 => putinmodeltype
    case 8 => requestmode
    case 9 => adprice
    case 10 => adppprice
    case 11 => requestdate
    case 12 => ip
    case 13 => appid
    case 14 => appname
    case 15 => uuid
    case 16 => device
    case 17 => client
    case 18 => osversion
    case 19 => density
    case 20 => pw
    case 21 => ph
    case 22 => long
    case 23 => lat
    case 24 => provincename
    case 25 => cityname
    case 26 => ispid
    case 27 => ispname
    case 28 => networkmannerid
    case 29 => networkmannername
    case 30 => iseffective
    case 31 => isbilling
    case 32 => adspacetype
    case 33 => adspacetypename
    case 34 => devicetype
    case 35 => processnode
    case 36 => apptype
    case 37 => district
    case 38 => paymode
    case 39 => isbid
    case 40 => bidprice
    case 41 => winprice
    case 42 => iswin
    case 43 => cur
    case 44 => rate
    case 45 => cnywinprice
    case 46 => imei
    case 47 => mac
    case 48 => idfa
    case 49 => openudid
    case 50 => androidid
    case 51 => rtbprovince
    case 52 => rtbcity
    case 53 => rtbdistrict
    case 54 => rtbstreet
    case 55 => storeurl
    case 56 => realip
    case 57 => isqualityapp
    case 58 => bidfloor
    case 59 => aw
    case 60 => ah
    case 61 => imeimd5
    case 62 => macmd5
    case 63 => idfamd5
    case 64 => openudidmd5
    case 65 => androididmd5
    case 66 => imeisha1
    case 67 => macsha1
    case 68 => idfasha1
    case 69 => openudidsha1
    case 70 => androididsha1
    case 71 => uuidunknow
    case 72 => userid
    case 73 => iptype
    case 74 => initbidprice
    case 75 => adpayment
    case 76 => agentrate
    case 77 => lomarkrate
    case 78 => adxrate
    case 79 => title
    case 80 => keywords
    case 81 => tagid
    case 82 => callbackdate
    case 83 => channelid
    case 84 => mediatype
  }

  override def productArity: Int = 85

  override def canEqual(that: Any): Boolean = that.isInstanceOf[DMP]
}