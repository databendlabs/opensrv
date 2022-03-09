use crate::packet::*;

#[test]
fn test_one_ping() {
    assert_eq!(
        onepacket(&[0x01, 0, 0, 0, 0x10]).unwrap().1,
        (0, &[0x10][..])
    );
}

#[test]
fn test_ping() {
    let p = packet(&[0x01, 0, 0, 0, 0x10]).unwrap().1;
    assert_eq!(p.0, 0);
    assert_eq!(&*p.1, &[0x10][..]);
}

#[test]
fn test_long_exact() {
    let mut data = vec![0xff, 0xff, 0xff, 0];
    data.extend(&[0; U24_MAX][..]);
    data.push(0x00);
    data.push(0x00);
    data.push(0x00);
    data.push(1);

    let (rest, p) = packet(&data[..]).unwrap();
    assert!(rest.is_empty());
    assert_eq!(p.0, 1);
    assert_eq!(p.1.len(), U24_MAX);
    assert_eq!(&*p.1, &[0; U24_MAX][..]);
}

#[test]
fn test_long_more() {
    let mut data = vec![0xff, 0xff, 0xff, 0];
    data.extend(&[0; U24_MAX][..]);
    data.push(0x01);
    data.push(0x00);
    data.push(0x00);
    data.push(1);
    data.push(0x10);

    let (rest, p) = packet(&data[..]).unwrap();
    assert!(rest.is_empty());
    assert_eq!(p.0, 1);
    assert_eq!(p.1.len(), U24_MAX + 1);
    assert_eq!(&p.1[..U24_MAX], &[0; U24_MAX][..]);
    assert_eq!(&p.1[U24_MAX..], &[0x10]);
}
