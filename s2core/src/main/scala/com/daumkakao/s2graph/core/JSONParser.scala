package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.models.{HLabelMeta, HLabel}
import play.api.libs.json._

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * helper to convert json into DataVal
 */
trait JSONParser {
  import DataVal._
  def innerValToJsValue(innerVal: DataVal): JsValue = {
    innerVal.dataType match {
      case BOOLEAN => JsBoolean(innerVal.toVal[Boolean])
      case BYTE => JsNumber(BigDecimal(innerVal.toVal[Byte]))
      case SHORT => JsNumber(BigDecimal(innerVal.toVal[Short]))
      case INT => JsNumber(BigDecimal(innerVal.toVal[Int]))
      case LONG => JsNumber(BigDecimal(innerVal.toVal[Long]))
      case FLOAT => JsNumber(BigDecimal(innerVal.toVal[Float]))
      case DOUBLE => JsNumber(BigDecimal(innerVal.toVal[Double]))
      case STRING => JsString(innerVal.toVal[String])
      case _ => throw new Exception(s"InnerVal should be [long/integeer/short/byte/string/boolean]")
    }
  }

  def toInnerVal(s: String, dataType: String) = {
    dataType match {
      case BOOLEAN => DataVal.withBoolean(s.toBoolean)
      case BYTE => DataVal.withByte(s.toByte)
      case SHORT => DataVal.withShort(s.toShort)
      case INT => DataVal.withInt(s.toInt)
      case LONG => DataVal.withLong(s.toLong)
      case FLOAT => DataVal.withFloat(s.toFloat)
      case DOUBLE => DataVal.withDouble(s.toDouble)
      case STRING => DataVal.withString(s)
      case _ =>
      case _ => throw new Exception(s"InnerVal should be [long/integeer/short/byte/string/boolean]")
    }
  }

  def toInnerVal(jsValue: JsValue): DataVal = {
    jsValue match {
      case b: JsBoolean => DataVal.withBoolean(b.value)
      case n: JsNumber =>
        n.value match {
          case b: Byte => DataVal.withByte(b)
          case s: Short => DataVal.withShort(s)
          case i: Int => DataVal.withInt(i)
          case l: Long => DataVal.withLong(l)
          case f: Float => DataVal.withFloat(f)
          case d: Double => DataVal.withDouble(d)
          case _ => throw new Exception(s"InnerVal should be [long/integeer/short/byte/string/boolean]")
        }
      case s: JsString => DataVal.withString(s.value)
      case _ => throw new Exception("JsonValue should be in [long/string/boolean].")
    }
  }
  def jsValueToInnerVal(jsValue: JsValue, dataType: String): Option[DataVal] = {
    val dType = dataType.toLowerCase()
    val ret = try {
      jsValue match {
        case n: JsNumber =>
          dType match {
            case x if !List(BYTE, SHORT, INT, LONG, FLOAT, DOUBLE).contains(x) => None
            case _ => Some(toInnerVal(jsValue))
          }
        case s: JsString =>
          dType match {
            case STRING => Some(toInnerVal(jsValue))
            case _ => None
          }
        case b: JsBoolean =>
          dType match {
            case BOOLEAN => Some(toInnerVal(jsValue))
            case _ => None
          }
        case _ => None
      }
    } catch {
      case e: Throwable =>
//        Logger.error(s"$jsValue -> $dataType")
        None
    }

    ret
  }
  def innerValToString(innerVal: DataVal, dataType: String): String = {
    dataType.toLowerCase() match {
      case STRING => JsString(innerVal.toString).toString
      case _ => innerVal.toString
    }
  }
  case class WhereParser(label: HLabel) extends JavaTokenParsers with JSONParser {

    val metaProps = label.metaPropsInvMap ++ Map(HLabelMeta.from.name -> HLabelMeta.from, HLabelMeta.to.name -> HLabelMeta.to)

    def where: Parser[Where] = rep(clause) ^^ (Where(_))

    def clause: Parser[Clause] = (predicate | parens) * (
      "and" ^^^ { (a: Clause, b: Clause) => And(a, b) } |
        "or" ^^^ { (a: Clause, b: Clause) => Or(a, b) })

    def parens: Parser[Clause] = "(" ~> clause <~ ")"

    def boolean = ("true" ^^^ (true) | "false" ^^^ (false))

    /** floating point is not supported yet **/
    def predicate = (
      (ident ~ "=" ~ ident | ident ~ "=" ~ decimalNumber | ident ~ "=" ~ stringLiteral) ^^ {
        case f ~ "=" ~ s =>
          metaProps.get(f) match {
            case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
            case Some(metaProp) =>
              Equal(metaProp.seq, toInnerVal(s, metaProp.dataType))
          }
      }
        | (ident ~ "between" ~ ident ~ "and" ~ ident | ident ~ "between" ~ decimalNumber ~ "and" ~ decimalNumber
        | ident ~ "between" ~ stringLiteral ~ "and" ~ stringLiteral) ^^ {
        case f ~ "between" ~ minV ~ "and" ~ maxV =>
          metaProps.get(f) match {
            case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
            case Some(metaProp) =>
              Between(metaProp.seq, toInnerVal(minV, metaProp.dataType), toInnerVal(maxV, metaProp.dataType))
          }
      }
        | (ident ~ "in" ~ "(" ~ rep(ident | decimalNumber | stringLiteral | "true" | "false" | ",") ~ ")") ^^ {
        case f ~ "in" ~ "(" ~ vals ~ ")" =>
          metaProps.get(f) match {
            case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
            case Some(metaProp) =>
              val values = vals.filter(v => v != ",").map { v =>
                toInnerVal(v, metaProp.dataType)
              }
              IN(metaProp.seq, values.toSet)
          }
      }
        | (ident ~ "!=" ~ ident | ident ~ "!=" ~ decimalNumber | ident ~ "!=" ~ stringLiteral) ^^ {
        case f ~ "!=" ~ s =>
          metaProps.get(f) match {
            case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
            case Some(metaProp) =>
              Not(Equal(metaProp.seq, toInnerVal(s, metaProp.dataType)))
          }
      }
        | (ident ~ "not in" ~ "(" ~ rep(ident | decimalNumber | stringLiteral | "true" | "false" | ",") ~ ")") ^^ {
        case f ~ "not in" ~ "(" ~ vals ~ ")" =>
          metaProps.get(f) match {
            case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
            case Some(metaProp) =>
              val values = vals.filter(v => v != ",").map { v =>
                toInnerVal(v, metaProp.dataType)
              }
              Not(IN(metaProp.seq, values.toSet))
          }
      }
      )

    def parse(sql: String): Option[Where] = {
      parseAll(where, sql) match {
        case Success(r, q) => Some(r)
        case x => println(x); None
      }
    }
  }
}