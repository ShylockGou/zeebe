/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.test;

import io.zeebe.test.util.MsgPackUtil;

public class MsgPackConstants {

  public static final String NODE_STRING_PATH = "$.string";
  public static final String NODE_JSON_OBJECT_PATH = "$.jsonObject";
  public static final String NODE_ROOT_PATH = "$";

  public static final String JSON_DOCUMENT = "{'string':'value', 'jsonObject':{'testAttr':'test'}}";
  public static final String OTHER_DOCUMENT = "{'string':'bar', 'otherObject':{'testAttr':'test'}}";
  public static final String MERGED_OTHER_WITH_JSON_DOCUMENT =
      "{'string':'bar', 'jsonObject':{'testAttr':'test'}, 'otherObject':{'testAttr':'test'}}";
  public static final byte[] MSGPACK_PAYLOAD;
  public static final byte[] OTHER_PAYLOAD;

  static {
    MSGPACK_PAYLOAD = MsgPackUtil.asMsgPack(JSON_DOCUMENT);
    OTHER_PAYLOAD = MsgPackUtil.asMsgPack(OTHER_DOCUMENT);
  }
}
