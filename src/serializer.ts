
export class Serializer {
  static pack(obj: object) {
    return Serializer._pack(obj);
  }

  static unpack(raw: string) {
    return Serializer._unpack(raw);
  }

  private static tag(str: string) {
    // force invalid JSON-like string, replaced when unpacking
    return `""${str}""`;
  }

  private static esc(str: string, addQuotes: boolean) {
    // escape stringified template literals and ctx functions for eval
    return (addQuotes || /[^a-zA-Z0-9]/.test(str)) ? JSON.stringify(str) : str;
  }

  private static _pack(obj: any): string {
    if (Array.isArray(obj)) {
      return `[${obj.map(Serializer._pack).join(',')}]`;
    }
    else if (Object.prototype.toString.call(obj) === '[object Object]') {
      const fields = Object.keys(obj)
        .map(k => `${Serializer.esc(k, false)}:${Serializer._pack(obj[k])}`)
        .join(',');

      return `{${fields}}`;
    }
    else if (typeof obj === 'string') {
      return `${Serializer.esc(obj, true)}`;
    }
    else if (typeof obj === 'number') {
      if (obj === Number.POSITIVE_INFINITY) {
        return Serializer.tag('Infinity');
      }
      else if (obj === Number.NEGATIVE_INFINITY) {
        return Serializer.tag('-Infinity');
      }
      else if (Number.isNaN(obj)) {
        return Serializer.tag('NaN');
      }
      else {
        return `${obj}`;
      }
    }
    else if (obj instanceof Date) {
      // return tagStr(obj.toString()); // keeps tz, loses ms
      return Serializer.tag(obj.toISOString()); // loses tz, keeps ms
    }
    else if (obj === undefined) {
      return Serializer.tag('undefined');
    }
    else if (obj === null) {
      return 'null';
    }
    else if (typeof obj === 'boolean') {
      return obj ? 'true' : 'false';
    }
    else if (typeof obj === 'function') { // drop
      return 'null';
    }
    else if (typeof obj === 'symbol') { // drop
      return 'null';
    }
    else {
      throw new Error('invalid pack value');
    }
  }

  private static _unpack(packed: string): object {
    const objStr = packed
      .replace(/""Infinity""/g, 'Infinity')
      .replace(/""-Infinity""/g, '-Infinity')
      .replace(/""undefined""/g, 'undefined')
      .replace(/""NaN""/g, 'NaN')
      .replace(/""([^"]+?)""/g, 'new Date("$1")');

    // TODO: write a proper parser to replace eval
    return eval(`(${objStr})`);
  }
}
