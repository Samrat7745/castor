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
//	CastorUploadClient.connectWebSocket / disconnectWebSocket              -> UploadClient.Connect / Disconnect
//	CastorUploadClient.uploadTupleChunk(chunk[, timeout])                  -> UploadClient.UploadTupleChunk (false on failure or timeout, like Java)
//	CastorUploadClient.activateTupleChunk(id)                              -> UploadClient.ActivateTupleChunk
//	DefaultCastorUploadClient.builder(address)                             -> NewUploadClient(Options)
//
// Errors map to Java's CastorClientException and are returned as *ClientError.
package client
