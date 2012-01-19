package com.reportgrid.util

sealed trait Unapply[A, B] {
  def unapply(b: B): A
}

trait Bijection[A, B] extends Function1[A, B] with Unapply[A, B] 

sealed class Biject[A](a: A) {
  def as[B](implicit f: Either[Bijection[A, B], Bijection[B, A]]): B = f.fold(_ apply a, _ unapply a)
}

trait Bijections {
  implicit def biject[A](a: A): Biject[A] = new Biject(a)
  implicit def forwardEither[A, B](implicit a: Bijection[A,B]): Either[Bijection[A,B], Bijection[B,A]] = Left(a)
  implicit def reverseEither[A, B](implicit b: Bijection[B,A]): Either[Bijection[A,B], Bijection[B,A]] = Right(b)
}

object Bijection extends Bijections 
