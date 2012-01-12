package com.reportgrid.common.json

sealed trait SJValue

sealed trait StorageType[A <: SJValue] {
  def as(v: SJValue)(implicit m: ClassManifest[A]): Option[A] = v match {
    case v if v.getClass.isAssignableFrom(m.erasure) => Some(v.asInstanceOf[A])
    case _ => None
  }
}

case class SJObject(fields: Map[String, SJValue]) extends SJValue
case object SJObject extends StorageType[SJObject] with (Map[String, SJValue] => SJObject)

case class SJArray(values: Vector[SJValue]) extends SJValue
case object SJArray extends StorageType[SJArray] with (Vector[SJValue] => SJArray)

case class SJString(value: String) extends SJValue
case object SJString extends StorageType[SJString] with (String => SJString)

case class SJBoolean(value: Boolean) extends SJValue
case object SJBoolean extends StorageType[SJBoolean] with (Boolean => SJBoolean)

case class SJLong(value: Long) extends SJValue
case object SJLong extends StorageType[SJLong] with (Long => SJLong)

case class SJDouble(value: Double) extends SJValue
case object SJDouble extends StorageType[SJDouble] with (Double => SJDouble)

case class SJDecimal(value: BigDecimal) extends SJValue
case object SJDecimal extends StorageType[SJDecimal] with (BigDecimal => SJDecimal)

case object SJNull extends SJValue with StorageType[SJValue]

case object SJEmptyObject extends StorageType[SJObject]
case object SJEmptyArray extends StorageType[SJArray]
