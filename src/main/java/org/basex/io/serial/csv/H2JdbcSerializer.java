package org.basex.io.serial.csv;

import org.basex.build.csv.CsvOptions;
import org.basex.build.csv.CsvOptions.CsvFormat;
import org.basex.io.serial.SerializerOptions;
import org.basex.query.util.ft.FTPos;
import org.basex.query.value.item.QNm;
import org.basex.util.Util;
import org.basex.util.XMLToken;
import org.basex.util.hash.TokenMap;
import org.basex.util.list.TokenList;
import org.h2.engine.Session;
import org.h2.table.Column;
import org.h2.tools.SimpleResultSet;
import org.h2.value.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Types;

import static org.basex.query.QueryError.CSV_SERIALIZE_X;
import static org.basex.util.Token.*;

/**
 * This class serializes items as CSV.
 *
 * @author BaseX Team 2005-19, BSD License
 * @author Christian Gruen
 */
public final class H2JdbcSerializer extends CsvSerializer {
    /** Names of header elements. */
    private final TokenList headers;
    /** Attribute format. */
    private final boolean atts;
    /** Lax flag. */
    private final boolean lax;

    /** Contents of current row. */
    private TokenMap data;
    /** Current attribute value. */
    private byte[] attv;

    private SimpleResultSet resultSet;

    private boolean headerType;

    private Column[] columns;

    private Session session;
    /**
     * Constructor.
     * @param opts serialization parameters
     * @throws IOException I/O exception
     */
    public H2JdbcSerializer(final SimpleResultSet resultSet, final SerializerOptions opts,
                            boolean containsHeaderType, Session session) throws IOException {
        super(null, opts);
        headers = header ? new TokenList() : null;
        atts = copts.get(CsvOptions.FORMAT) == CsvFormat.ATTRIBUTES;
        lax = copts.get(CsvOptions.LAX) || atts;
        this.resultSet = resultSet;
        headerType = containsHeaderType;
        this.session = session;
    }

    @Override
    protected void startOpen(final QNm name) {
        if(level == 1) data = new TokenMap();
        attv = null;
    }

    @Override
    protected void finishEmpty() throws IOException {
        finishOpen();
        if(level == 2) cache(EMPTY);
        finishClose();
    }

    @Override
    protected void text(final byte[] value, final FTPos ftp) throws IOException {
        if(level == 3) cache(value);
    }

    @Override
    protected void finishClose() throws IOException {
        if(level != 1) return;

        final TokenList tl = new TokenList();
        if(headers != null) {
            final int size = headers.size();
            // print header
            String[] headerNames = null;
            if(header) {
                for(int i = 0; i < size; i++) tl.add(headers.get(i));
                headerNames = tokenToString(tl);
                if(!headerType){
                    for(String column: headerNames){
                        resultSet.addColumn(column, Types.VARCHAR, -1, -1);
                    }
                }
                header = false;
            }
            // print data, sorted by headers
            for(int i = 0; i < size; i++) tl.add(data.get(headers.get(i)));
            if(headerType){
                fillResponseColumnType(tl, headerNames);
                headerType=false;
            } else {
                fillResultSetRow(tokenToString(tl));
            }
        } else {
            // no headers available: print data
            for(final byte[] value : data.values()) tl.add(value);
            fillResultSetRow(tokenToString(tl));
        }
    }

    private String[] tokenToString(TokenList tl) throws UnsupportedEncodingException {
        String[] value = new String[tl.size()];
        for(int idx = 0; idx < tl.size(); idx++){
            value[idx] = tl.get(idx)!=null?new String(tl.get(idx), StandardCharsets.UTF_8.name()):null;
        }
        tl.reset();
        return value;
    }

    @Override
    protected void attribute(final byte[] name, final byte[] value, final boolean standalone) {
        attv = value;
    }

    /**
     * Caches the specified text and its header.
     * @param value text to be cached
     * @throws IOException I/O exception
     */
    private void cache(final byte[] value) throws IOException {
        if(headers != null) {
            final byte[] key = atts && attv != null ? attv : elem.string();
            final byte[] name = XMLToken.decode(key, lax);
            if(name == null) throw CSV_SERIALIZE_X.getIO(Util.inf("Invalid element name <%>", key));
            if(!headers.contains(name)) headers.add(name);
            final byte[] old = data.get(name);
            data.put(name, old == null || old.length == 0 ? value :
                    value.length == 0 ? old : concat(old, ',', value));
        } else {
            data.put(token(data.size()), value);
        }
    }

    @Override
    public void close() throws IOException {
    }

    private void fillResponseColumnType(TokenList tl, String[] headerNames) throws UnsupportedEncodingException {
        String[] columnType = tokenToString(tl);
        ColumnTypeParser columnTypeParser = new ColumnTypeParser();
        columns = new Column[headerNames.length];
        for (int idx = 0; idx < columnType.length; idx++) {
            String typeString = columnType[idx].toUpperCase();
            String headerName = headerNames[idx];
            Column type = columnTypeParser.getColumnType(headerName, typeString);
            columns[idx] = type;
            TypeInfo typeType = type.getType();
            int sqlType = DataType.convertTypeToSQLType(typeType.getValueType());
            resultSet.addColumn(headerName, sqlType, (int) typeType.getPrecision(), typeType.getScale());
        }
    }

    private void fillResultSetRow(String[] data) {
        if(columns.length!=data.length){
            throw new IllegalArgumentException("Data column count "+data.length+
                    " is not match column definition count "+columns.length);
        }
        Object[] row = new Object[data.length];
        for (int idx = 0; idx < data.length; idx++) {
            Column column = columns[idx];
            Value value = data[idx]!=null ?
                    column.convert(ValueString.get(data[idx],true)): ValueNull.INSTANCE;
            column.validateConvertUpdateSequence(session, value);
            row[idx] = value;
        }
        resultSet.addRow(row);
    }
}
