import io.carbynestack.castor.common.entities.*;
import io.carbynestack.castor.common.websocket.UploadTupleChunkResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationUtils;
import java.io.*;
import java.nio.file.*;
import java.util.*;

/** Generates golden fixtures (Java serialization + Jackson JSON) for the Go port. */
public class Gen {
  static Path out;
  static void w(String name, byte[] b) throws Exception { Files.write(out.resolve(name), b); }
  public static void main(String[] a) throws Exception {
    out = Paths.get(a[0]);
    Files.createDirectories(out);
    UUID id = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    byte[] data = new byte[32 * 3];
    for (int i = 0; i < data.length; i++) data[i] = (byte) i;
    TupleChunk c = TupleChunk.of(Bit.class, Field.GFP, id, data);
    w("tuplechunk_bit_gfp.bin", SerializationUtils.serialize(c));
    byte[] d2 = new byte[96 * 2];
    for (int i = 0; i < d2.length; i++) d2[i] = (byte) (i * 7);
    TupleChunk c2 = TupleChunk.of(MultiplicationTriple.class, Field.GF2N, id, new byte[48 * 2]);
    w("tuplechunk_mt_gf2n.bin", SerializationUtils.serialize(c2));
    TupleChunk empty = TupleChunk.of(Bit.class, Field.GFP, id, new byte[0]);
    w("tuplechunk_empty.bin", SerializationUtils.serialize(empty));
    w("resp_success.bin", SerializationUtils.serialize(UploadTupleChunkResponse.success(id)));
    w("resp_failure_id.bin", SerializationUtils.serialize(UploadTupleChunkResponse.failure(id, "Failed writing tuple chunk to database.")));
    w("resp_failure_null.bin", SerializationUtils.serialize(UploadTupleChunkResponse.failure(null, "Unsupported payload type.")));
    UUID id2 = UUID.fromString("00000000-0000-0000-0000-00000000ffff");
    List<ReservationElement> els = new ArrayList<>();
    els.add(new ReservationElement(id, 10, 0));
    els.add(new ReservationElement(id2, 5, 1000));
    Reservation r = new Reservation(id + "_bit_gfp", TupleType.BIT_GFP, els);
    w("reservation_locked.bin", SerializationUtils.serialize(r));
    Reservation r2 = new Reservation(id + "_multiplicationtriple_gf2n", TupleType.MULTIPLICATION_TRIPLE_GF2N, new ArrayList<>(els.subList(0, 1)));
    r2.setStatus(ActivationStatus.UNLOCKED);
    w("reservation_unlocked.bin", SerializationUtils.serialize(r2));
    w("long_42.bin", SerializationUtils.serialize(Long.valueOf(42L)));
    w("long_big.bin", SerializationUtils.serialize(Long.valueOf(1L << 40)));
    ObjectMapper om = new ObjectMapper();
    w("reservation.json", om.writeValueAsBytes(r));
    byte[] raw = new byte[96 * 2];
    for (int i = 0; i < raw.length; i++) raw[i] = (byte) (i + 1);
    TupleList parsed = TupleList.fromStream(MultiplicationTriple.class, Field.GFP, new ByteArrayInputStream(raw), raw.length);
    w("tuplelist_mt_gfp.json", om.writeValueAsBytes(parsed));
    w("tuplelist_mt_gfp.raw", raw);
    byte[] raw2 = new byte[16 * 2];
    for (int i = 0; i < raw2.length; i++) raw2[i] = (byte) (200 + i);
    TupleList p2 = TupleList.fromStream(Bit.class, Field.GF2N, new ByteArrayInputStream(raw2), raw2.length);
    w("tuplelist_bit_gf2n.json", om.writeValueAsBytes(p2));
    w("tuplelist_bit_gf2n.raw", raw2);
    List<TupleMetric> ms = new ArrayList<>();
    for (TupleType t : TupleType.values()) ms.add(TupleMetric.of(1000, 3, t));
    w("telemetry.json", om.writeValueAsBytes(new TelemetryData(ms, 60000)));
    System.out.println("ok");
  }
}
