import breeze.linalg._
import org.apache.spark.mllib.linalg.{DenseVector => SDV, SparseVector => SSV, Vector => SV}
import org.apache.spark.mllib.linalg.{DenseMatrix => SDM, SparseMatrix => SSM, Matrix => SM}

object MatrixUtil extends java.io.Serializable {
def transpose(arr: Array[Array[Double]]): Array[Array[Double]] = {
  val mat = new Array[Array[Double]](arr.head.length)
  for (i <- arr.head.indices) {
    mat(i) = arr.map(_(i))
  }
  mat
}

def matToRowArrs(mat: SM): Array[Array[Double]] = {
  val arrs = new Array[Array[Double]](mat.rows)
  for (r <- 0 until mat.rows) {
    arrs(r) = toBreeze(mat)(r to r, 0 to mat.cols - 1).toDenseMatrix.toArray
  }
  arrs
}

def matToRowArrs(mat: Matrix[Double]): Array[Array[Double]] = {
  val arrs = new Array[Array[Double]](mat.rows)
  for (r <- 0 until mat.rows) {
    arrs(r) = mat(r to r, 0 to mat.cols - 1).toDenseMatrix.toArray
  }
  arrs
}

def arrsToMat(arrs: Iterator[Array[Double]]): DenseMatrix[Double] = {
  vecArrsToMats(arrs, arrs.length).next()
}

def vecArrsToMats(vecArrs: Iterator[Array[Double]], chunkSize: Int)
: Iterator[DenseMatrix[Double]] = {
new Iterator[DenseMatrix[Double]] {
def hasNext: Boolean = vecArrs.hasNext
def next(): DenseMatrix[Double] = {
val firstVec = vecArrs.next()
val vecLen : Int = firstVec.length
val arr = new Array[Double](chunkSize * vecLen)
System.arraycopy(firstVec, 0, arr, 0, vecLen)
var i : Int = 1
var offs : Int = 0
while (i < chunkSize && vecArrs.hasNext) {
  val vec = vecArrs.next()
  System.arraycopy(vec, 0, arr, offs, vecLen)
  i += 1
  offs += vecLen
}
new DenseMatrix[Double](i, firstVec.length, arr)
}
}
}

def fromBreeze(breeze: Matrix[Double]): SM = {
  breeze match {
  case dm: DenseMatrix[Double] =>
  new SDM(dm.rows, dm.cols, dm.data, dm.isTranspose)
  case sm: CSCMatrix[Double] =>
  // There is no isTranspose flag for sparse matrices in Breeze
  new SSM(sm.rows, sm.cols, sm.colPtrs, sm.rowIndices, sm.data)
  case _ =>
  throw new UnsupportedOperationException(
  s"Do not support conversion from type ${breeze.getClass.getName}.")
  }
}

def toBreeze(sparkMatrix: SM): Matrix[Double] = {
sparkMatrix match {
case dm: SDM =>
if (!dm.isTransposed) {
new DenseMatrix[Double](dm.numRows, dm.numCols, dm.values)
} else {
val breezeMatrix = new DenseMatrix[Double](dm.numCols, dm.numRows, dm.values)
breezeMatrix.t
}
case sm: SSM =>
if (!sm.isTransposed) {
new CSCMatrix[Double](sm.values, sm.numRows, sm.numCols, sm.colPtrs, sm.rowIndices)
} else {
val breezeMatrix =
new CSCMatrix[Double](sm.values, sm.numCols, sm.numRows, sm.colPtrs, sm.rowIndices)
breezeMatrix.t
}
case _ =>
throw new UnsupportedOperationException(
s"Do not support conversion from type ${sparkMatrix.getClass.getName}.")
}
}
def fromBreeze(breezeVector: Vector[Double]): SV = {
breezeVector match {
case v: DenseVector[Double] =>
if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
new SDV(v.data)
} else {
 new SDV(v.toArray)  // Can't use underlying array directly, so make a new one
}
case v: SparseVector[Double] =>
if (v.index.length == v.used) {
 new SSV(v.length, v.index, v.data)
} else {
new SSV(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
}
case v: SliceVector[_, Double] =>
new SDV(v.toArray)
case v: Vector[_] =>
sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
}
}
def toBreeze(sparkVector: SV): Vector[Double] = {
sparkVector match {
case v: SDV =>
new DenseVector[Double](v.values)
case v: SSV =>
new SparseVector[Double](v.indices, v.values, v.size)
}
}
implicit def mSparkToBreeze(sparkMatrix: SM): Matrix[Double] =
toBreeze(sparkMatrix)
implicit def dmSparkToBreeze(sparkMatrix: SDM): DenseMatrix[Double] =
toBreeze(sparkMatrix).asInstanceOf[DenseMatrix[Double]]
implicit def mBreezeToSpark(breezeMatrix: Matrix[Double]): SM =
fromBreeze(breezeMatrix)
implicit def dmBreezeToSpark(breezeMatrix: DenseMatrix[Double]): SDM =
fromBreeze(breezeMatrix).asInstanceOf[SDM]
implicit def vSparkToBreeze(sparkVector: SV): Vector[Double] =
toBreeze(sparkVector)
implicit def dvSparkToBreeze(sparkVector: SDV): DenseVector[Double] =
toBreeze(sparkVector).asInstanceOf[DenseVector[Double]]
implicit def vBreezeToSpark(breezeVector: Vector[Double]): SV =
fromBreeze(breezeVector)
implicit def dvBreezeToSpark(breezeVector: DenseVector[Double]): SDV =
fromBreeze(breezeVector).asInstanceOf[SDV]
implicit def fvtovBreezeToSpark(f: (Vector[Double]) => Vector[Double])
: (SV) => SV = {
v: SV => f(v)
}
}

