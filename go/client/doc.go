// Package client is the Go replacement for castor-java-client.
//
// Java -> Go
//
//	CastorIntraVcpClient.downloadTupleShares(requestId, tupleType, count)  -> IntraVcpClient.DownloadTupleShares
//	CastorIntraVcpClient.getTelemetryData()                                -> IntraVcpClient.GetTelemetryData
//	CastorIntraVcpClient.getTelemetryData(interval)                        -> IntraVcpClient.GetTelemetryDataFor
//	CastorInterVcpClient.shareReservation(reservation)                     -> InterVcpClient.ShareReservation
//	CastorInterVcpClient.updateReservationStatus(id, status)               -> InterVcpClient.UpdateReservationStatus
//	DefaultCastor*VcpClient.builder(address)                               -> NewIntraVcpClient / NewInterVcpClient (+ Options)
//
// Errors map to Java's CastorClientException and are returned as *ClientError.
package client
