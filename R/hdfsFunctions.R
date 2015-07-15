#' read data from hdfs as a dataframe in R
#'
#' @param hdfsUri the namenode of your hdfs, just as hdfs://192.163.8.1:9000
#' @param src the file or directory in your hdfs. if it is a directory, the funciton will get all files in the directroy, an read them as a dataframe, and if there are other directories in src, they will not read.
#' @param sep the separator character.
#' @param ... the arguments can be pass to read.table.
#' @export
copyMergeToLocal <- function(hdfsUri, src,...){
	dst = paste(getwd(), "temp.tmp",sep="/")
	HDFSCopyMerge <- .jnew("hdfs/HDFSCopyMerge")
	.jcall(HDFSCopyMerge, "V", "copyMergeToLocal", hdfsUri, src, dst)
	dataRes = read.table(dst,...)
	return(dataRes)
}
	
