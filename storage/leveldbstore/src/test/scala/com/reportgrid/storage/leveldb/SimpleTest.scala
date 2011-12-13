package reportgrid.storage.leveldb

object SimpleTest {
  def main (argv : Array[String]) {
    val c = new Column("test", "/tmp")

    c.insert(12364534l, BigDecimal("1.445322").underlying)
  }
}

// vim: set ts=4 sw=4 et:
